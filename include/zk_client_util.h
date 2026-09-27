#pragma once

#include <zookeeper/zookeeper.h>
#include <semaphore.h>
#include <string>
#include <vector>

class ZKClient
{
public:
    ZKClient();
    ~ZKClient();

    // 本类持有 zhandle 与连接同步用的信号量，禁止拷贝：
    // 复制会导致同一句柄被两次 zookeeper_close、以及 context 指向错误的信号量
    ZKClient(const ZKClient&) = delete;
    ZKClient& operator=(const ZKClient&) = delete;

    // 启动zkclient 连接zkserver
    void start();
    // 主动关闭连接。会**立即**删除本客户端注册的临时节点（不必等 30s 会话超时），
    // 这是优雅退出的关键一步：服务进程收到 SIGINT 后主动调它，元数据服务的节点轮询
    // 下一拍就能摘掉这个节点。幂等；不调用则由析构兜底。
    void close();
    // 在zkserver上指定路径创建znode节点
    void create(const std::string path, const std::string data="", bool isEphemeral = false);
    // 读取节点数据的结果：调用方据此区分两种「返回空串」——节点不存在 vs 读失败
    enum GetResult
    {
        GET_OK,
        GET_NOT_FOUND,
        GET_ERROR
    };
    // 获取指定节点路径的值（按节点实际长度读取，**不会**被截断）。
    // 节点不存在或读失败时返回空串，用 result 出参区分两者——不要靠空串判断。
    std::string getData(const std::string& path, GetResult* result = nullptr);
    // 获取指定节点路径 的 所有孩子名字
    std::vector<std::string> getChildren(const std::string path);
private:
    // zk客户端句柄
    zhandle_t* m_zhandle;
    // 连接同步用信号量。**必须是成员**：start() 会把它取址存进 zhandle 的 context，
    // watcher 线程在会话事件里对该地址 sem_post。若用栈上局部变量，start() 一返回
    // 地址即失效，watcher 之后投递任何会话事件（例如 zookeeper_close 期间）都是往
    // 已销毁的内存写，表现为偶发 SIGSEGV。
    sem_t m_sem;
};
