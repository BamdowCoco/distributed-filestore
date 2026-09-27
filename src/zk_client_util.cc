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
    close();
    sem_destroy(&m_sem);
}

void ZKClient::close()
{
    if(m_zhandle) {
        // 关闭句柄 释放资源。这会先停掉 watcher 与 I/O 线程，
        // 因此必须在 sem_destroy 之前完成，否则 watcher 可能向已销毁的信号量投递。
        // 同时 ZK 会立刻删除本会话创建的临时节点——这正是优雅退出想要的效果。
        zookeeper_close(m_zhandle);
        m_zhandle = nullptr;
    }
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
//
// 路径分两类，**已存在**对它们的含义完全不同，必须分开判断（原实现一律只打一行 INFO 就继续）：
//   - 父路径（`/service`、`/service/method`）是非临时的，多个节点抢建属正常：先到者建、后到者复用；
//   - 临时节点 hostPath（`/service/method/ip:port`）已存在，意味着**这个地址已被另一个存活会话占用**，
//     本进程其实**没有注册成功**。若静默放过，就会出现"进程一切正常、日志无异常，ZK 里却没有它"
//     的故障：元数据把它从环上摘掉，它却照常监听——N9 实测过这一幕（旧集群的临时节点尚未随
//     会话超时消失，新集群抢建拿到"已存在"，于是三个新存储节点全部从未注册）。
//     故临时节点冲突一律**明确失败退出**：宁可起不来，也不要当一个不在册的服务节点。
//     同一文件里 `zookeeper_init` 失败、连接超时本就是 exit，语义一致。
void ZKClient::create(const std::string path, const std::string data, bool isEphemeral)
{
    if(nullptr == m_zhandle) {
        LOG_ERROR("failed to create node, please execute ZKClient::start()! path:%s", path.c_str());
        return;
    }

    char path_buffer[128] = {0};
    int path_buffer_len = sizeof(path_buffer);

    int flag = zoo_exists(m_zhandle, path.c_str(), 0, nullptr);
    if(ZOK == flag) {
        if(isEphemeral) {
            LOG_ERROR("ephemeral node already exists, address claimed by another live session! path:%s", path.c_str());
            exit(EXIT_FAILURE);
        }
        LOG_INFO("parent node exists, reuse it. path:%s", path.c_str());
        return;
    }
    if(ZNONODE != flag) {
        // 既不是"存在"也不是"不存在"（会话失效、连接断开……）：同样不能当作成功
        LOG_ERROR("failed to stat node before create! path:%s flag:%d", path.c_str(), flag);
        exit(EXIT_FAILURE);
    }

    // 节点不存在，创建
    flag = zoo_create(m_zhandle, path.c_str(), data.c_str(), data.size(),
    &ZOO_OPEN_ACL_UNSAFE, (isEphemeral) ? 1 : 0, path_buffer, path_buffer_len);
    if(ZOK == flag) {
        LOG_INFO("create node success! path:%s", path.c_str());
        return;
    }
    if(ZNODEEXISTS == flag) {
        // zoo_exists 与 zoo_create 之间非原子：父路径被别人抢先建好属正常，复用即可；
        // 临时节点冲突则同上——这是一次失败的注册，不能吞掉。
        if(isEphemeral) {
            LOG_ERROR("ephemeral node claimed concurrently by another session! path:%s", path.c_str());
            exit(EXIT_FAILURE);
        }
        LOG_INFO("parent node already created by peer! path:%s", path.c_str());
        return;
    }
    LOG_ERROR("failed to create node! path:%s flag:%d", path.c_str(), flag);
    exit(EXIT_FAILURE);
}

// 获取指定节点路径的值
//
// 为什么不能给 zoo_get 一个固定大小的栈缓冲区：zoo_get 在数据超过缓冲区时会
// **静默截断**，并把 *buffer_len 置为截断后的长度（见 zookeeper.c 的 COMPLETION_DATA
// 分支：len = min(res.data.len, buff_len)）。旧实现用 `char buffer[64]` 且忽略返回
// 的长度，任何 ≥64 字节的值都会被读成残缺串——而且不会报错。
// 这里改为先 zoo_exists 拿 Stat.dataLength（唯一可信的长度），再按需分配读取。
std::string ZKClient::getData(const std::string& path, GetResult* result)
{
    if(nullptr == m_zhandle) {
        LOG_ERROR("failed to get node data, please execute ZKClient::start()! path:%s", path.c_str());
        if (result != nullptr) {
            *result = GET_ERROR;
        }
        return std::string();
    }

    struct Stat stat;
    int flag = zoo_exists(m_zhandle, path.c_str(), 0, &stat);
    if (ZNONODE == flag) {
        // 节点不存在不是错误，但对调用方是「没有数据」——必须与读失败区分开
        if (result != nullptr) {
            *result = GET_NOT_FOUND;
        }
        return std::string();
    }
    if (ZOK != flag) {
        LOG_ERROR("failed to stat node! path:%s flag:%d", path.c_str(), flag);
        if (result != nullptr) {
            *result = GET_ERROR;
        }
        return std::string();
    }

    int32_t len = stat.dataLength;
    if (len <= 0) {
        if (result != nullptr) {
            *result = GET_OK;
        }
        return std::string();
    }

    std::vector<char> buffer(static_cast<size_t>(len) + 1, '\0');
    int bufferLen = len;
    flag = zoo_get(m_zhandle, path.c_str(), 0, buffer.data(), &bufferLen, nullptr);
    if (ZOK != flag) {
        LOG_ERROR("failed to get node data! path:%s flag:%d", path.c_str(), flag);
        if (result != nullptr) {
            *result = GET_ERROR;
        }
        return std::string();
    }
    if (bufferLen != len) {
        // 两次调用之间数据被改动（长度已变），宁可报错让调用方重试，也不要返回半截内容。
        // 注意此处**不打印数据内容**：节点值可能是凭据类信息，不该进日志。
        LOG_ERROR("node data changed while reading! path:%s expect:%d got:%d",
                  path.c_str(), len, bufferLen);
        if (result != nullptr) {
            *result = GET_ERROR;
        }
        return std::string();
    }

    if (result != nullptr) {
        *result = GET_OK;
    }
    return std::string(buffer.data(), static_cast<size_t>(bufferLen));
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