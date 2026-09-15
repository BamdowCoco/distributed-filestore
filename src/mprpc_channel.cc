#include "mprpc_channel.h"
#include "rpcheader.pb.h"
#include "mprpc_application.h"
#include "logger.h"
#include "zk_client_util.h"

#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <string>

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>

// 重写CallMethod
// 所有stub代理对象调用rpc方法都会调用该函数
// 统一做rpc请求序列化、网络发送、接收响应、rpc响应反序列化
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                              google::protobuf::RpcController* controller,
                              const google::protobuf::Message* request,
                              google::protobuf::Message* response, google::protobuf::Closure* done)
{ 
    // headerSize
    // header: serviceName+methodName+argsSize
    // args: request序列化
    
    // request序列化
    std::string argsStr = request->SerializeAsString();
    
    // 定义rpc请求header
    std::string serviceName(method->service()->name());
    std::string methodName(method->name());

    mprpc::RpcHeader header;
    header.set_service_name(serviceName);
    header.set_method_name(methodName);
    header.set_args_size(argsStr.size());
    std::string headerStr = header.SerializeAsString();

    int32_t headerSize = headerStr.size();

    // 组装待发送的rpc请求字符串
    std::string sendRpcStr;
    sendRpcStr.append((char*)&headerSize, 4);
    sendRpcStr.append(headerStr);
    sendRpcStr.append(argsStr);

    LOG_INFO("headerSize:%d", headerSize);
    LOG_INFO("service_name:%s", serviceName.c_str());
    LOG_INFO("method_name:%s", methodName.c_str());
    // LOG_INFO("headerStr:%s", headerStr.c_str());
    // LOG_INFO("argsStr:%s", argsStr.c_str());


    // TCP 发送rpc请求
    // 创建套接字 -> 发起连接请求 -> 发送数据 -> 接收响应数据 -> 断开连接
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == fd) {
        std::string reason = "failed to create socket! errno: " + std::to_string(errno);
        LOG_ERROR("%s", reason.c_str());
        controller->SetFailed(reason);
        return;
    }

    // 获取目标节点的 ip 和 端口
    std::string ip;
    uint16_t port;
    bool isConnSuccess = false;

    if (m_useDirectConn) {
        // 直连模式：跳过 ZooKeeper 服务发现，直接连接指定节点
        ip = m_targetIp;
        port = m_targetPort;

        struct sockaddr_in rpcServerAddr;
        rpcServerAddr.sin_family = AF_INET;
        rpcServerAddr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &rpcServerAddr.sin_addr.s_addr);

        if (0 == connect(fd, (struct sockaddr*)&rpcServerAddr, sizeof(rpcServerAddr))) {
            isConnSuccess = true;
        }
    } else {
        // 服务发现模式：向 zk 获取对应服务下方法节点的 ip 和 端口
        ZKClient zkClient;
        zkClient.start();
        std::string methodPath = '/' + serviceName + '/' + methodName;
        /* 一个方法能由多个服务器提供 */
        std::vector<std::string> hostStrVec = zkClient.getChildren(methodPath);
        for (std::string& hostStr : hostStrVec) {
            size_t colonIdx = hostStr.find(':');
            if (colonIdx == std::string::npos) {
                controller->SetFailed("method:" + methodPath + " address invalid!");
                continue;
            }
            ip = hostStr.substr(0, colonIdx);
            port = std::stoi(hostStr.substr(colonIdx + 1));

            struct sockaddr_in rpcServerAddr;
            rpcServerAddr.sin_family = AF_INET;
            rpcServerAddr.sin_port = htons(port);
            inet_pton(AF_INET, ip.c_str(), &rpcServerAddr.sin_addr.s_addr);

            if (0 == connect(fd, (struct sockaddr*)&rpcServerAddr, sizeof(rpcServerAddr))) {
                // 连接成功 退出循环
                isConnSuccess = true;
                break;
            }
        }
    }

    if (isConnSuccess) {
        // 连接成功
        LOG_INFO("connect success! ip:%s port:%d", ip.c_str(), port);
    } else {
        // 连接失败
        std::string reason = "failed to connect rpc server! errno: " + std::to_string(errno);
        LOG_ERROR("%s", reason.c_str());
        controller->SetFailed(reason);
        close(fd);
        return;
    }

    if(0 >= send(fd, sendRpcStr.c_str(), sendRpcStr.size(), 0)) {
        std::string reason = "failed to send rpc request! errno: " + std::to_string(errno);
        LOG_ERROR("%s", reason.c_str());
        controller->SetFailed(reason);
        close(fd);
        return;
    }

    // 接收rpc响应（循环读取直到连接关闭，支持任意大小的响应）
    std::string responseStr;
    char buf[1024] = {0};
    int recvSize = 0;
    while ((recvSize = recv(fd, buf, sizeof(buf), 0)) > 0) {
        responseStr.append(buf, recvSize);
    }

    // 断开连接
    close(fd);

    if (responseStr.empty()) {
        std::string reason = "failed to receive rpc response! errno: " + std::to_string(errno);
        LOG_ERROR("%s", reason.c_str());
        controller->SetFailed(reason);
        return;
    }

    LOG_INFO("recvSize:%zu", responseStr.size());

    // rpc响应反序列化
    if (!response->ParseFromString(responseStr)) {
        std::string reason = "failed to parseFromString rpc response! size:" + std::to_string(responseStr.size());
        LOG_ERROR("%s", reason.c_str());
        controller->SetFailed(reason);
        return;
    }

}