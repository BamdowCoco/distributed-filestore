#pragma once
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <unordered_map>
#include <string>
#include <mutex>

#include "thread_pool.h"
#include "zk_client_util.h"


// 框架提供的 专门发布rpc服务的网络对象类
class RpcProvider
{
public:
    // 提供给外部 发布rpc方法的接口
    void notifyService(google::protobuf::Service* service);

    // 启动rpc服务节点 开始提供rpc远程过程调用网络服务。
    // **收到 SIGINT/SIGTERM 后会返回**（signalfd 唤醒事件循环），返回前已关闭 ZK 连接
    // （临时节点立即删除）并停掉业务线程池。
    void run();

private:
    muduo::net::EventLoop m_eventLoop;

    // 本节点向 ZK 注册的句柄。**成员而非 run() 的局部对象**：run() 现在会在收到信号后
    // 返回，需要在返回前显式 close() 让临时节点立刻消失；并且它必须**早于** m_workerPool
    // 被析构（成员按逆序析构）——先停线程池、再断 ZK，保证没有 handler 还在用连接。
    ZKClient m_zkClient;

    // 业务处理器线程池：handler 不再跑在 I/O 线程上，避免一次慢查询 / 大块 MD5 / 磁盘 IO
    // 阻塞该 loop 上的所有连接（head-of-line blocking）。
    //
    // **必须声明在 m_eventLoop 之后**：成员按逆序析构，线程池要先于 EventLoop 停止。
    // run() 返回前会显式 m_workerPool.stop()，那是真正生效的停止路径。
    ThreadPool m_workerPool;

    // service 服务信息
    struct ServiceInfo
    {
        google::protobuf::Service* m_service; // 服务对象
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor*> m_methodMap; // 服务的方法
    };
    // 存储服务对象及其方法的信息
    std::unordered_map<std::string, ServiceInfo> m_serviceInfoMap;
    // std::unordered_map<std::string, google::protobuf::Service*> m_serviceMap;

    // 处理连接回调函数
    void onConnection(const muduo::net::TcpConnectionPtr& conn);

    // 处理读写事件回调函数
    void onMessage(const muduo::net::TcpConnectionPtr& conn,
                   muduo::net::Buffer* buffer,
                   muduo::Timestamp time);

    // Closure回调函数 用于序列化rpc响应并发送回客户端
    //
    // 形参是**按值**的 TcpConnectionPtr，不能改成 const&：见 .cc 里 NewCallback 处的说明——
    // protobuf 的闭包会把模板参数原样存成成员，写成引用就只存引用、闭包不持有连接所有权，
    // 而 handler 现在在工作线程上执行，闭包可能在 onMessage 返回之后才跑。
    void sendRpcResponse(muduo::net::TcpConnectionPtr conn, const google::protobuf::Message* response);

    // 扫描并关闭空闲超时的连接（P18，由 run() 的定时器周期性触发）
    void checkIdleConnections();

    // 连接的在途任务完成后递减计数（在连接所属 loop 线程上执行）
    void decrementInFlight(const muduo::net::TcpConnectionPtr& conn);

    // P18 空闲连接追踪：连接名 -> 连接对象 + 最后活跃时间（onMessage 更新，定时器线程扫描）
    struct ConnectionInfo
    {
        muduo::net::TcpConnectionPtr conn;
        muduo::Timestamp lastActivity;
        // 已投递但尚未回响应的 handler 数。>0 表示 handler 正在工作线程里执行，
        // 此时**不得**按空闲关闭——否则会把响应从 handler 脚下截掉。
        int inFlight = 0;
    };
    std::mutex m_connMutex;
    std::unordered_map<std::string, ConnectionInfo> m_connections;
};