#include "mprpc_provider.h"
#include "logger.h"
#include "mprpc_application.h"
#include "rpcheader.pb.h"
#include "zk_client_util.h"

#include <functional>
#include <cstring>
#include <vector>
#include <muduo/base/Timestamp.h>

// P18 空闲连接超时：超过该时长无请求则关闭（会话式长连接下清理僵尸连接）
constexpr double kIdleTimeout = 30.0;
// P18 空闲扫描间隔
constexpr double kIdleCheckInterval = 5.0;

/*
service_name => service描述 => Service* 服务对象
服务下的method_name => method方法对象
*/
// 提供给外部 发布rpc方法的接口
void RpcProvider::notifyService(google::protobuf::Service* service)
{
    ServiceInfo serviceInfo;
    serviceInfo.m_service = service;

    // 获取服务对象的描述信息
    const google::protobuf::ServiceDescriptor* serviceDescPtr = service->GetDescriptor();

    // 获取服务的名字
    std::string serviceName(serviceDescPtr->name());
    // std::string serviceFullName(serviceDescPtr->full_name());
    
    LOG_INFO("serviceName:%s", serviceName.c_str());    
    
    // 获取服务对象 方法数量
    int methodCount = serviceDescPtr->method_count();
    LOG_INFO("methodCount:%d", methodCount);
    for (int i=0; i<methodCount; i++) {
        // 获取服务对象对应下标的服务方法描述信息
        const google::protobuf::MethodDescriptor* methodDescPtr = serviceDescPtr->method(i);
        std::string methodName(methodDescPtr->name());
        LOG_INFO("methodName:%s", methodName.c_str());    
        serviceInfo.m_methodMap.insert({methodName, methodDescPtr});
    }

    m_serviceInfoMap.insert({serviceName, serviceInfo});
    // m_serviceMap.insert({serviceName, service});
}

// 启动rpc服务节点 开始提供rpc远程过程调用网络服务
void RpcProvider::run()
{
    std::string ip = MprpcApplication::getConfig().load("rpc_server_ip");
    uint16_t port = std::stoi(MprpcApplication::getConfig().load("rpc_server_port"));
    muduo::net::InetAddress addr(ip, port);

    // 创建TcpServer对象
    muduo::net::TcpServer server(&m_eventLoop, addr, "RpcProvider");

    // 注册连接回调 和 消息读写回调
    // 分离了网络通信和业务处理模块
    server.setConnectionCallback(std::bind(&RpcProvider::onConnection, this, std::placeholders::_1));
    server.setMessageCallback(std::bind(&RpcProvider::onMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

    // 设置专门负责通信的I/O线程池大小 即不包括监听连接的主线程
    server.setThreadNum(4);

    LOG_INFO("RpcProvider start service ip:%s port:%d", ip.c_str(), port);

    // 把当前节点的服务和方法 注册到ZooKeeper上
    ZKClient zkClient;
    zkClient.start();
    for(auto& serviceInfoPair : m_serviceInfoMap) {
        //  /service_name
        std::string servicePath = '/'+serviceInfoPair.first;
        zkClient.create(servicePath);
        for(auto& methodPtrPair : serviceInfoPair.second.m_methodMap) {
            // /service_name/method_name
            std::string methodPath = servicePath + '/'+methodPtrPair.first;
            zkClient.create(methodPath);
            // 创建临时性节点 /service_name/method_name/ip:port
            std::string hostPath = methodPath+'/'+ip+':'+std::to_string(port);
            zkClient.create(hostPath, "", true);
        }
    }

    // 未使用unordered_map 缓存 method
    // for(auto& servicePtr : m_serviceMap) {
    //     //  /service_name
    //     std::string servicePath = '/'+servicePtr.first;
    //     zkClient.create(servicePath);
    //     auto serviceDescPtr = servicePtr.second->GetDescriptor();
    //     int methodCnt = serviceDescPtr->method_count();
    //     for(int i=0;i<methodCnt;i++) {
    //         // /service_name/method_name
    //         auto methodDescPtr = serviceDescPtr->method(i);
    //         std::string methodPath = servicePath+'/'+std::string(methodDescPtr->name());
    //         zkClient.create(methodPath);
    //         // 创建临时节点 /service_name/method_name/ip:port
    //         std::string hostPath = methodPath+'/'+ip+':'+std::to_string(port);
    //         zkClient.create(hostPath, "", true);
    //     }
    // }



    // 周期性扫描空闲连接，关闭超时僵尸连接（P18）
    m_eventLoop.runEvery(kIdleCheckInterval, std::bind(&RpcProvider::checkIdleConnections, this));

    // 启动网络服务
    server.start();
    m_eventLoop.loop();

}

// 处理连接回调函数
void RpcProvider::onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    if (conn->connected()) {
        // 登记连接，记录最后活跃时间（P18 空闲超时追踪）
        std::lock_guard<std::mutex> lock(m_connMutex);
        m_conns[conn->name()] = conn;
        m_lastActivity[conn->name()] = muduo::Timestamp::now();
    } else {
        // 移除追踪
        {
            std::lock_guard<std::mutex> lock(m_connMutex);
            m_conns.erase(conn->name());
            m_lastActivity.erase(conn->name());
        }
        // 断开与rpc客户端连接
        conn->shutdown();
    }
}

// P18 扫描并关闭空闲超时的连接（由 run() 的定时器周期性触发，运行在 m_eventLoop 线程）
void RpcProvider::checkIdleConnections()
{
    std::vector<muduo::net::TcpConnectionPtr> toShutdown;
    muduo::Timestamp now = muduo::Timestamp::now();
    {
        std::lock_guard<std::mutex> lock(m_connMutex);
        for (const auto& kv : m_conns) {
            auto it = m_lastActivity.find(kv.first);
            if (it != m_lastActivity.end() &&
                muduo::timeDifference(it->second, now) > kIdleTimeout) {
                toShutdown.push_back(kv.second);
            }
        }
    }
    for (const auto& conn : toShutdown) {
        LOG_INFO("close idle connection: %s", conn->name().c_str());
        conn->shutdown();
    }
}

// 处理读写事件回调函数
// 若远程有rpc调用请求, 会调用该回调函数
/*
在框架内部 RpcProvider(callee)和RpcConsumer(caller)协商好通信使用的protobuf数据类型
service_name method_name args
在proto定义message类型 进行数据头序列化和反序列化

header_size(4字节)
header: service_name method_name args_size
args

*/
void RpcProvider::onMessage(const muduo::net::TcpConnectionPtr& conn,
               muduo::net::Buffer* buffer,
               muduo::Timestamp time)
{
    // 更新最后活跃时间（P18 空闲超时追踪）
    {
        std::lock_guard<std::mutex> lock(m_connMutex);
        m_lastActivity[conn->name()] = time;
    }

    // TCP 是字节流，大请求（如 PutChunksBatch 批量上传）可能被拆成多次 onMessage 回调（半包），
    // 故逐帧解析：先用 peek 判断「header_size(4B) + header + args」是否收全，收全才取走处理，否则等下次回调。
    while (buffer->readableBytes() >= 4) {
        // 读取header_size（前4字节，主机字节序，与客户端 append 一致）
        int32_t headerSize = 0;
        std::memcpy(&headerSize, buffer->peek(), 4);
        if (headerSize <= 0 || headerSize > 65536) {
            LOG_ERROR("invalid headerSize:%d", headerSize);
            conn->shutdown();
            return;
        }

        size_t headerTotal = 4 + static_cast<size_t>(headerSize);
        if (buffer->readableBytes() < headerTotal) {
            return;   // header 半包，等下次回调
        }

        // 解析header，得到 service_name / method_name / args_size
        mprpc::RpcHeader rpcHeader;
        if (!rpcHeader.ParseFromArray(buffer->peek() + 4, headerSize)) {
            LOG_ERROR("failed to parse from string to rpcHeader!");
            conn->shutdown();
            return;
        }
        std::string serviceName = rpcHeader.service_name();
        std::string methodName = rpcHeader.method_name();
        int32_t argsSize = rpcHeader.args_size();
        if (argsSize < 0 || argsSize > 64 * 1024 * 1024) {
            LOG_ERROR("invalid argsSize:%d", argsSize);
            conn->shutdown();
            return;
        }

        size_t total = headerTotal + static_cast<size_t>(argsSize);
        if (buffer->readableBytes() < total) {
            return;   // args 半包，等下次回调
        }

        // 完整请求到达：跳过 header_size + header，取出 args
        buffer->retrieve(headerTotal);
        std::string argsStr = buffer->retrieveAsString(argsSize);

        LOG_INFO("headerSize:%d", headerSize);
        LOG_INFO("serviceName:%s", serviceName.c_str());
        LOG_INFO("methodName:%s", methodName.c_str());
        LOG_INFO("argsSize:%d", argsSize);

        // 获取service对象和method对象
        auto serviceInfoIt = m_serviceInfoMap.find(serviceName);
        if (serviceInfoIt == m_serviceInfoMap.end()) {
            LOG_ERROR("failed to find service:%s in m_serviceInfoMap!", serviceName.c_str());
            conn->shutdown();
            return;
        }

        auto& methodMap = serviceInfoIt->second.m_methodMap;
        auto methodIt = methodMap.find(methodName);
        if (methodIt == methodMap.end()) {
            LOG_ERROR("failed to find method:%s in methodMap!", methodName.c_str());
            conn->shutdown();
            return;
        }

        google::protobuf::Service* service = serviceInfoIt->second.m_service;
        const google::protobuf::MethodDescriptor* method = methodIt->second;

        // 生成rpc远程过程调用的请求request和响应response
        google::protobuf::Message* request = service->GetRequestPrototype(method).New();
        if (!request->ParseFromString(argsStr)) {
            LOG_ERROR("failed to parse from string to request! content:%s", argsStr.c_str());
            delete request;
            conn->shutdown();
            return;
        }
        google::protobuf::Message* response = service->GetResponsePrototype(method).New();

        // 绑定Closure回调函数
        google::protobuf::Closure* done =
            google::protobuf::NewCallback<RpcProvider,
                                          const muduo::net::TcpConnectionPtr&,
                                          const google::protobuf::Message*>(this,
                                                                            &RpcProvider::sendRpcResponse,
                                                                            conn,
                                                                            response);

        // 执行相应的rpc方法
        service->CallMethod(method, nullptr, request, response, done);

        // request 生命周期结束（handler 同步执行完毕），释放，避免大请求（批量上传）泄漏
        delete request;
    }
}

// Closure回调函数 用于序列化rpc响应并发送回客户端
void RpcProvider::sendRpcResponse(const muduo::net::TcpConnectionPtr& conn, const google::protobuf::Message* response)
{
    // rpc响应序列化
    std::string responseStr;
    if (!response->SerializeToString(&responseStr)) {
        LOG_ERROR("failed to serialize to string ! content:%s", responseStr.c_str());
        conn->shutdown();
        delete response;   // 序列化失败也需释放
        return;
    }

    // 响应加 4 字节长度前缀，客户端按长度读（长连接复用）
    int32_t respSize = responseStr.size();
    std::string sendStr;
    sendStr.append((char*)&respSize, 4);
    sendStr.append(responseStr);

    // 将响应发送到rpc调用端
    conn->send(sendStr);
    // 响应后关闭连接（短连接），避免服务器累积空闲连接
    conn->shutdown();

    // 释放 response，避免批量大响应（批量下载）泄漏
    delete response;
}