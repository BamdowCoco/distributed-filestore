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
    // 在zkserver上指定路径创建znode节点
    void create(const std::string path, const std::string data="", bool isEphemeral = false);
    // 获取指定节点路径的值
    std::string getData(const std::string path);
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
