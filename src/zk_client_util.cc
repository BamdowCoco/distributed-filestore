#include "zk_client_util.h"
#include "mprpc_application.h"
#include "logger.h"

#include <semaphore.h>

// 全局watcher回调
// zkserver给zkclient响应通知
void globalWatcher(zhandle_t *zh, int type,
                int state, const char *path,void *watcherCtx)
{
    if(type != ZOO_SESSION_EVENT || state != ZOO_CONNECTED_STATE) {
        return;
    }
    // context 是 ZKClient 的成员信号量；关闭流程中可能已清空，必须判空
    sem_t* sem = (sem_t*)zoo_get_context(zh);
    if (sem != nullptr) {
        sem_post(sem);
    }
}

ZKClient::ZKClient():m_zhandle(nullptr)
{
    sem_init(&m_sem, 0, 0);
}

ZKClient::~ZKClient()
{
    if(m_zhandle) {
        // 关闭句柄 释放资源。这会先停掉 watcher 与 I/O 线程，
        // 因此必须在 sem_destroy 之前完成，否则 watcher 可能向已销毁的信号量投递
        zookeeper_close(m_zhandle);
        m_zhandle = nullptr;
    }
    sem_destroy(&m_sem);
}

// 启动zkclient 连接zkserver
void ZKClient::start()
{
    std::string ip = MprpcApplication::getConfig().load("zookeeper_ip");
    std::string port = MprpcApplication::getConfig().load("zookeeper_port");
    std::string connstr = ip + ':' + port;

    /*
    zookeeper_mt - zk多线程版本
    1. API调用线程(当前线程)
    2. 网络I/O线程 底层实现:pthread_create poll
    3. watcher回调线程(globalWatcher) 底层实现:pthread_create
    */
    // 异步创建zk句柄 API
    m_zhandle = zookeeper_init(connstr.c_str(), globalWatcher, 30000, nullptr, nullptr, 0);
    if(nullptr == m_zhandle) {
        LOG_ERROR("zookeeper_init error!");
        exit(EXIT_FAILURE);
    }

    // 把成员信号量的地址交给 zhandle 作为 context（存活期与句柄一致，见头文件说明）
    zoo_set_context(m_zhandle, &m_sem);

    struct timespec abstime;

    // 获取当前时间 +3s 作为绝对时间
    clock_gettime(CLOCK_REALTIME, &abstime);
    abstime.tv_sec += 3;

    if(0 == sem_timedwait(&m_sem, &abstime)) {
        LOG_INFO("zkclient start success!");
    } else if(errno == ETIMEDOUT){
        LOG_ERROR("zkclient start wait timeout(3s) failure! errno: %d", errno);
        exit(EXIT_FAILURE);
    } else {
        LOG_ERROR("zkclient start UNKNOWN error! errno: %d", errno);
        exit(EXIT_FAILURE);
    }
}

// 在zkserver上指定路径创建znode节点
void ZKClient::create(const std::string path, const std::string data, bool isEphemeral)
{
    if(nullptr == m_zhandle) {
        LOG_ERROR("failed to create node, please execute ZKClient::start()! path:%s", path.c_str());
    }

    char path_buffer[128] = {0};
    int path_buffer_len = sizeof(path_buffer);
    int flag;
    flag = zoo_exists(m_zhandle, path.c_str(), 0, nullptr);
    if(ZNONODE == flag) {
        // 当前节点不存在 创建
        flag = zoo_create(m_zhandle, path.c_str(), data.c_str(), data.size(), 
        &ZOO_OPEN_ACL_UNSAFE, (isEphemeral) ? 1 : 0, path_buffer, path_buffer_len);
        if(ZOK == flag) {
            LOG_INFO("create node success! path:%s", path.c_str());
        } else if(ZNODEEXISTS == flag) {
            // 竞态：zoo_exists 与 zoo_create 之间非原子，另一进程可能已创建该节点，
            // 此时视为「节点已存在、目的已达成」继续，而不是 exit
            LOG_INFO("node already created by peer! path:%s", path.c_str());
        } else {
            LOG_ERROR("failed to create node! path:%s flag:%d", path.c_str(), flag);
            exit(EXIT_FAILURE);
        }
    }
    LOG_INFO("current node exists! path:%s", path.c_str());
}

// 获取指定节点路径的值
std::string ZKClient::getData(const std::string path)
{
    if(nullptr == m_zhandle) {
        LOG_ERROR("failed to get node data, please execute ZKClient::start()! path:%s", path.c_str());
        return std::string();
    }
    
    char buffer[64] = {0};
    int bufferLen = sizeof(buffer);
    int flag;
    flag = zoo_get(m_zhandle, path.c_str(), 0, buffer, &bufferLen, nullptr);
    if(ZOK != flag) {
        LOG_ERROR("failed to get node data! path:%s flag:%d", path.c_str(), flag);
        return std::string();
    }
    LOG_INFO("get node data success! path:%s data:%s", path.c_str(), buffer);
    return std::string(buffer);
}

// 获取指定节点路径 的 所有孩子名字
std::vector<std::string> ZKClient::getChildren(const std::string path)
{
    if(nullptr == m_zhandle) {
        LOG_ERROR("failed to get node data, please execute ZKClient::start()! path:%s", path.c_str());
        return {};
    }

    int zoo_get_children(zhandle_t *zh, const char *path, int watch,
                            struct String_vector *strings);
    struct String_vector childrenStrings;
    int flag = zoo_get_children(m_zhandle, path.c_str(), 0, &childrenStrings);
    if(ZOK != flag) {
        LOG_ERROR("failed to get node's children! path:%s flag:%d", path.c_str(), flag);
        return {};
    }

    std::vector<std::string> childrenRet;
    for(int i=0;i<childrenStrings.count;i++) {
        childrenRet.push_back(childrenStrings.data[i]);
    }
    // zoo_get_children 在堆上分配了字符串数组，须显式释放
    deallocate_String_vector(&childrenStrings);

    return childrenRet;
}