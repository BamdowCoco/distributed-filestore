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

// 从 fd 循环读取恰好 n 字节，返回 false 表示连接中断/读不足
static bool recvAll(int fd, void* buf, size_t n)
{
    size_t got = 0;
    char* p = static_cast<char*>(buf);
    while (got < n) {
        ssize_t r = recv(fd, p + got, n - got, 0);
        if (r <= 0) {
            return false;
        }
        got += static_cast<size_t>(r);
    }
    return true;
}

// 建立 TCP 连接；成功返回 fd，失败返回 -1
static int tcpConnect(const std::string& ip, uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        return -1;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr.s_addr);
    if (0 != connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr))) {
        close(fd);
        return -1;
    }
    return fd;
}

// 客户端主动关闭会话内复用的连接
void MprpcChannel::closeConnection()
{
    if (m_fd != -1) {
        close(m_fd);
        m_fd = -1;
    }
}

MprpcChannel::~MprpcChannel()
{
    closeConnection();
}

// 发送请求并按 4 字节长度前缀接收响应
bool MprpcChannel::sendRecv(int fd, const std::string& sendRpcStr, std::string& responseStr,
                            google::protobuf::RpcController* controller)
{
    if (0 >= send(fd, sendRpcStr.c_str(), sendRpcStr.size(), 0)) {
        std::string reason = "failed to send rpc request! errno: " + std::to_string(errno);
        LOG_ERROR("%s", reason.c_str());
        controller->SetFailed(reason);
        return false;
    }

    // 先读 4 字节响应长度
    int32_t respSize = 0;
    if (!recvAll(fd, &respSize, 4)) {
        std::string reason = "failed to receive response header! errno: " + std::to_string(errno);
        LOG_ERROR("%s", reason.c_str());
        controller->SetFailed(reason);
        return false;
    }
    if (respSize < 0 || respSize > 64 * 1024 * 1024) {
        std::string reason = "invalid response size: " + std::to_string(respSize);
        LOG_ERROR("%s", reason.c_str());
        controller->SetFailed(reason);
        return false;
    }

    // 读满响应体
    responseStr.resize(respSize);
    if (respSize > 0 && !recvAll(fd, &responseStr[0], respSize)) {
        std::string reason = "failed to receive response body! errno: " + std::to_string(errno);
        LOG_ERROR("%s", reason.c_str());
        controller->SetFailed(reason);
        return false;
    }

    LOG_INFO("recvSize:%d", respSize);
    return true;
}

// 重写CallMethod
// 所有stub代理对象调用rpc方法都会调用该函数
// 统一做rpc请求序列化、网络发送、接收响应、rpc响应反序列化
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                              google::protobuf::RpcController* controller,
                              const google::protobuf::Message* request,
                              google::protobuf::Message* response, google::protobuf::Closure* done)
{
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

    std::string responseStr;

    if (m_useDirectConn) {
        if (m_sessionReuse) {
            // 会话式连接：首次连接、后续复用；失败则关闭并置 -1 供下次重连
            if (m_fd == -1) {
                m_fd = tcpConnect(m_targetIp, m_targetPort);
                if (m_fd == -1) {
                    std::string reason = "failed to connect rpc server! ip:" + m_targetIp +
                                         " port:" + std::to_string(m_targetPort);
                    LOG_ERROR("%s", reason.c_str());
                    controller->SetFailed(reason);
                    return;
                }
            }
            if (!sendRecv(m_fd, sendRpcStr, responseStr, controller)) {
                closeConnection();
                return;
            }
            // 复用连接，不关闭；会话结束由 closeConnection/析构关闭
        } else {
            // 直连模式：短连接，每次 CallMethod 新建连接、发完即关
            int fd = tcpConnect(m_targetIp, m_targetPort);
            if (fd == -1) {
                std::string reason = "failed to connect rpc server! ip:" + m_targetIp +
                                     " port:" + std::to_string(m_targetPort);
                LOG_ERROR("%s", reason.c_str());
                controller->SetFailed(reason);
                return;
            }
            if (!sendRecv(fd, sendRpcStr, responseStr, controller)) {
                close(fd);
                return;
            }
            close(fd);  // 短连接：发完即关
        }
    } else {
        // 服务发现模式：向 zk 获取服务方法节点，逐个尝试连接（短连接）
        ZKClient zkClient;
        zkClient.start();
        std::string methodPath = '/' + serviceName + '/' + methodName;
        /* 一个方法能由多个服务器提供 */
        std::vector<std::string> hostStrVec = zkClient.getChildren(methodPath);

        int fd = -1;
        for (std::string& hostStr : hostStrVec) {
            size_t colonIdx = hostStr.find(':');
            if (colonIdx == std::string::npos) {
                continue;
            }
            std::string ip = hostStr.substr(0, colonIdx);
            uint16_t port = std::stoi(hostStr.substr(colonIdx + 1));

            fd = tcpConnect(ip, port);
            if (fd != -1) {
                LOG_INFO("connect success! ip:%s port:%d", ip.c_str(), port);
                break;
            }
        }

        if (fd == -1) {
            std::string reason = "failed to connect rpc server! errno: " + std::to_string(errno);
            LOG_ERROR("%s", reason.c_str());
            controller->SetFailed(reason);
            return;
        }

        if (!sendRecv(fd, sendRpcStr, responseStr, controller)) {
            close(fd);
            return;
        }
        close(fd);  // 服务发现模式：短连接
    }

    // rpc响应反序列化
    if (!response->ParseFromString(responseStr)) {
        std::string reason = "failed to parseFromString rpc response! size:" + std::to_string(responseStr.size());
        LOG_ERROR("%s", reason.c_str());
        controller->SetFailed(reason);
        return;
    }
}
