#pragma once
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <unordered_map>
#include <string>
#include <mutex>


// 框架提供的 专门发布rpc服务的网络对象类
class RpcProvider
{
public:
    // 提供给外部 发布rpc方法的接口
    void notifyService(google::protobuf::Service* service);

    // 启动rpc服务节点 开始提供rpc远程过程调用网络服务
    void run();
    
private:
    muduo::net::EventLoop m_eventLoop;

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
    void sendRpcResponse(const muduo::net::TcpConnectionPtr& conn, const google::protobuf::Message* response);

    // 扫描并关闭空闲超时的连接（P18，由 run() 的定时器周期性触发）
    void checkIdleConnections();

    // P18 空闲连接追踪：连接名 -> 连接对象 / 最后活跃时间（onMessage 更新，定时器线程扫描）
    std::mutex m_connMutex;
    std::unordered_map<std::string, muduo::net::TcpConnectionPtr> m_conns;
    std::unordered_map<std::string, muduo::Timestamp> m_lastActivity;
};