#pragma once

#include <google/protobuf/service.h>
#include <string>
#include <cstdint>

class MprpcChannel: public google::protobuf::RpcChannel
{
public:
    // 默认构造：服务发现模式，通过 ZooKeeper 自动发现服务端点
    MprpcChannel() : m_useDirectConn(false), m_targetPort(0) {}

    // 直连模式：指定目标节点 ip/port，跳过 ZooKeeper 服务发现
    MprpcChannel(const std::string& ip, uint16_t port)
        : m_useDirectConn(true), m_targetIp(ip), m_targetPort(port) {}

    // 重写CallMethod
    // 所有stub代理对象调用rpc方法都会调用该函数
    // 统一做rpc请求序列化、网络发送、接收响应、rpc响应反序列化
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                          google::protobuf::RpcController* controller, const google::protobuf::Message* request,
                          google::protobuf::Message* response, google::protobuf::Closure* done) override;

private:
    // 通过 fd 发送请求并按 4 字节长度前缀接收响应
    bool sendRecv(int fd, const std::string& sendRpcStr, std::string& responseStr,
                  google::protobuf::RpcController* controller);

    bool m_useDirectConn;      // 是否直连模式
    std::string m_targetIp;    // 直连目标 ip
    uint16_t m_targetPort;     // 直连目标端口
};
