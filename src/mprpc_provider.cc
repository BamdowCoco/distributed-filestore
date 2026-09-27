#include "mprpc_provider.h"
#include "logger.h"
#include "mprpc_application.h"
#include "rpcheader.pb.h"
#include "zk_client_util.h"

#include <functional>
#include <cerrno>
#include <cstring>
#include <memory>
#include <vector>
#include <muduo/base/Timestamp.h>
#include <muduo/net/Channel.h>
#include <signal.h>
#include <sys/signalfd.h>
#include <unistd.h>

// P18 空闲连接超时：超过该时长无请求则关闭（会话式长连接下清理僵尸连接）
constexpr double kIdleTimeout = 30.0;
// P18 空闲扫描间隔
constexpr double kIdleCheckInterval = 5.0;

// 业务处理线程池默认大小（可用配置 rpc_worker_threads 覆盖）。
// 注意与 server.setThreadNum() 的 I/O 线程是两回事：后者只管网络收发，
// 这里跑的是业务 handler（MySQL/Redis/磁盘/MD5）。
constexpr int kDefaultWorkerThreads = 4;
// 在途任务上限（背压护栏，防止排队深度 × 单请求最大 64MB 无界增长）
constexpr size_t kMaxPendingTasks = 4096;
// 工作线程数上界。数量类配置即使语法合法、过大也会在运行时炸：
// 70000 会让线程池真的去创建 7 万个线程，std::thread 抛 std::system_error 且无人捕获 → abort。
constexpr int kMaxWorkerThreads = 256;

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
    std::string ip = MprpcApplication::getConfig().getString("rpc_server_ip");
    if (ip.empty()) {
        // 不默认绑 0.0.0.0：配置漏了就明确失败，而不是把服务暴露到所有网卡上
        LOG_ERROR("rpc_server_ip is not configured");
        exit(EXIT_FAILURE);
    }
    // 端口必须落在合法区间：getPositiveInt 已挡住"非数字"，这里再校验范围——
    // 否则 70000 会被静默截断成 4464（uint16_t）、缺失则变成 0（绑随机端口），
    // 两者都是"看起来起来了、实际监听到意料之外的端口"。
    // 端口没有合理默认值，所以这里选择**明确报错退出**（而不是回退默认值）。
    int portNum = MprpcApplication::getConfig().getPositiveInt("rpc_server_port", 0);
    if (portNum <= 0 || portNum > 65535) {
        LOG_ERROR("invalid rpc_server_port (must be 1..65535), got:%d", portNum);
        exit(EXIT_FAILURE);
    }
    uint16_t port = static_cast<uint16_t>(portNum);
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

    // 把当前节点的服务和方法 注册到ZooKeeper上。
    // m_zkClient 是成员（原先是局部对象）：它注册的临时节点必须在 run() 返回前主动关闭，
    // 让节点立刻消失，而不是等 ZK 会话超时（~30s）。
    m_zkClient.start();
    for(auto& serviceInfoPair : m_serviceInfoMap) {
        //  /service_name
        std::string servicePath = '/'+serviceInfoPair.first;
        m_zkClient.create(servicePath);
        for(auto& methodPtrPair : serviceInfoPair.second.m_methodMap) {
            // /service_name/method_name
            std::string methodPath = servicePath + '/'+methodPtrPair.first;
            m_zkClient.create(methodPath);
            // 创建临时性节点 /service_name/method_name/ip:port
            std::string hostPath = methodPath+'/'+ip+':'+std::to_string(port);
            m_zkClient.create(hostPath, "", true);
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

    // 启动业务处理线程池（handler 从这里起不再占用 I/O 线程）。
    // rpc_worker_threads=0 表示**明确禁用**：handler 退回在 I/O 线程上同步执行（旧行为）。
    m_workerPool.setMaxPending(kMaxPendingTasks);
    // 数量类配置用带区间的取值：过大直接回默认值，而不是让线程池去申请那么多线程
    int workerThreads = MprpcApplication::getConfig().getIntInRange(
        "rpc_worker_threads", kDefaultWorkerThreads, 0, kMaxWorkerThreads);
    if (workerThreads > 0) {
        m_workerPool.start(workerThreads);
    }
    LOG_INFO("worker threads:%d (0 = 禁用线程池，handler 在 I/O 线程上执行)", workerThreads);

    // 优雅退出：用 signalfd 把 SIGINT/SIGTERM 变成事件循环上的可读事件。
    //
    // 不在信号处理器里直接调 quit()：处理器里只允许做 async-signal-safe 的事，而
    // "唤醒事件循环"这一动作在 signalfd 方案下变成普通的 fd 可读回调，整段都在事件循环
    // 线程里执行，天然安全。
    //
    // 前提：SIGINT/SIGTERM 已被**屏蔽**（MprpcApplication::init() 在建任何线程之前做的）。
    // 阻塞后信号不再按默认处置终止进程，而是挂起为 pending，signalfd 即可读到它。
    sigset_t sigMask;
    sigemptyset(&sigMask);
    sigaddset(&sigMask, SIGINT);
    sigaddset(&sigMask, SIGTERM);
    int sigFd = ::signalfd(-1, &sigMask, SFD_NONBLOCK | SFD_CLOEXEC);
    std::unique_ptr<muduo::net::Channel> sigChannel;
    if (sigFd < 0) {
        // 拿不到 signalfd 只是失去优雅退出（退回旧的"被信号直接杀死"行为），不该拒绝服务
        LOG_ERROR("signalfd failed, graceful shutdown unavailable: %s", std::strerror(errno));
    } else {
        sigChannel.reset(new muduo::net::Channel(&m_eventLoop, sigFd));
        sigChannel->setReadCallback([this](muduo::Timestamp) { m_eventLoop.quit(); });
        sigChannel->enableReading();
    }

    // 启动网络服务
    server.start();
    m_eventLoop.loop();

    // 走到这里说明 quit() 被调用过 = 收到了 SIGINT/SIGTERM。
    // 先注销 Channel（它要去 eventLoop 的 poller 里摘 fd）再关 fd——Channel 不持有 fd 所有权。
    if (sigChannel) {
        sigChannel.reset();
        ::close(sigFd);
    }

    // 1) 关 ZK：zookeeper_close 会立刻删除本节点注册的**临时节点**，不必再等会话超时。
    //    这正是优雅退出的目的——元数据服务的 ZK 轮询下一拍就能把死节点从环上摘掉。
    m_zkClient.close();
    LOG_INFO("zk client closed, ephemeral nodes removed");

    // 2) 再停业务线程池：server 是 run() 的局部变量，其内部 I/O loop 即将析构，而工作线程
    //    持有的 conn->getLoop() 正指向它们——晚停就是悬垂指针。
    m_workerPool.stop();
    LOG_INFO("worker pool stopped, rpc provider exiting");
}

// 处理连接回调函数
void RpcProvider::onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    if (conn->connected()) {
        // 登记连接，记录最后活跃时间（P18 空闲超时追踪）
        std::lock_guard<std::mutex> lock(m_connMutex);
        m_connections[conn->name()] = ConnectionInfo{conn, muduo::Timestamp::now()};
    } else {
        // 移除追踪
        {
            std::lock_guard<std::mutex> lock(m_connMutex);
            m_connections.erase(conn->name());
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
        for (const auto& kv : m_connections) {
            const ConnectionInfo& info = kv.second;
            if (!info.conn) {
                continue;   // 防御：map 可能被默认插入过空连接
            }
            // handler 正在工作线程里执行时不得按空闲关闭——否则响应会被从 handler
            // 脚下截掉。这是把 handler 移出 I/O 线程后必须补的一环：原先 handler
            // 阻塞着 loop，定时器根本不可能在 handler 执行期间触发。
            if (info.inFlight > 0) {
                continue;
            }
            if (muduo::timeDifference(now, info.lastActivity) > kIdleTimeout) {
                toShutdown.push_back(info.conn);
            }
        }
    }
    for (const auto& conn : toShutdown) {
        LOG_INFO("close idle connection: %s", conn->name().c_str());
        conn->shutdown();
    }
}

// 在途任务完成：递减计数。marshal 回连接所属 loop 执行——m_connections 会被多个
// loop 线程并发访问，故一律加锁（同线程调用时 runInLoop 会立即执行）。
void RpcProvider::decrementInFlight(const muduo::net::TcpConnectionPtr& conn)
{
    conn->getLoop()->runInLoop([this, conn]() {
        std::lock_guard<std::mutex> lock(m_connMutex);
        auto it = m_connections.find(conn->name());
        if (it != m_connections.end() && it->second.inFlight > 0) {
            --it->second.inFlight;
        }
    });
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
        m_connections[conn->name()].lastActivity = time;
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

        // 每个连接最多一个在途请求。客户端是严格 lockstep 的（发一个、等一个响应），
        // 同一连接上并发第二个请求本身就是协议违约；强制这一点同时消除了
        // 「多个工作线程完成顺序不定 → 响应乱序」的风险。
        {
            std::lock_guard<std::mutex> lock(m_connMutex);
            auto connIt = m_connections.find(conn->name());
            if (connIt == m_connections.end()) {
                LOG_ERROR("connection not tracked, drop request. conn:%s", conn->name().c_str());
                conn->shutdown();
                return;
            }
            if (connIt->second.inFlight > 0) {
                LOG_ERROR("second in-flight request on one connection (protocol violation), closing. conn:%s",
                          conn->name().c_str());
                conn->shutdown();
                return;
            }
            ++connIt->second.inFlight;
        }

        // 生成rpc远程过程调用的请求request和响应response
        google::protobuf::Message* request = service->GetRequestPrototype(method).New();
        if (!request->ParseFromString(argsStr)) {
            LOG_ERROR("failed to parse from string to request! content:%s", argsStr.c_str());
            delete request;
            decrementInFlight(conn);
            conn->shutdown();
            return;
        }
        google::protobuf::Message* response = service->GetResponsePrototype(method).New();

        // 绑定Closure回调函数
        //
        // 这里踩过一个坑，值得记下来：protobuf 的 MethodClosure2 会把显式模板参数**原样**存成
        // 成员。原先写的是 `const TcpConnectionPtr&`，于是闭包里存的是一个**引用成员**——
        // 闭包并不持有连接的所有权。handler 在本函数内同步执行时那个引用一直有效，看不出问题；
        // 一旦 handler 被挪到工作线程（见下方 submit），闭包会在 onMessage 返回、muduo 的
        // 回调状态销毁之后才执行，引用随即悬垂，表现为 sendRpcResponse 里读 TcpConnection
        // 的 heap-use-after-free（由 ASAN 定位）。
        // 因此模板参数与方法形参都用**按值**的 TcpConnectionPtr：闭包持有一份 shared_ptr 拷贝，
        // 连接在响应真正发出前不会被释放。
        google::protobuf::Closure* done =
            google::protobuf::NewCallback<RpcProvider,
                                          muduo::net::TcpConnectionPtr,
                                          const google::protobuf::Message*>(this,
                                                                            &RpcProvider::sendRpcResponse,
                                                                            conn,
                                                                            response);

        // handler 执行体
        auto runHandler = [this, conn, service, method, request, response, done]() {
            // handler 必须同步执行完毕并调用 done->Run()（现有 handler 全部如此）。
            // 本框架不支持延迟响应——request/response 的生命周期依赖这个约定。
            try {
                service->CallMethod(method, nullptr, request, response, done);
            } catch (const std::exception& e) {
                // 单个请求里的异常不应终止整个进程（handler 会解析来自外部的数据）
                LOG_ERROR("handler threw an exception: %s", e.what());
                conn->getLoop()->runInLoop([conn]() { conn->shutdown(); });
            } catch (...) {
                LOG_ERROR("handler threw an unknown exception");
                conn->getLoop()->runInLoop([conn]() { conn->shutdown(); });
            }
            // request 生命周期结束（handler 同步执行完毕），释放，避免大请求泄漏；
            // response 由 sendRpcResponse 释放、done 由 protobuf 自删除，均不在此处理
            delete request;
            decrementInFlight(conn);
        };

        if (!m_workerPool.running()) {
            // 线程池被显式禁用（rpc_worker_threads=0）：退回原行为，在 I/O 线程上同步执行
            runHandler();
            continue;
        }
        if (!m_workerPool.submit(runHandler)) {
            // 已达背压上限：不执行该请求，就地释放并关闭连接。
            // 框架不知道 response 的具体类型，回不了结构化错误，只能关连接（客户端会重试）。
            LOG_ERROR("worker pool rejected request (pending:%zu), closing connection. conn:%s",
                      m_workerPool.pending(), conn->name().c_str());
            delete request;
            delete response;
            delete done;
            decrementInFlight(conn);
            conn->shutdown();
            return;
        }
    }
}

// Closure回调函数 用于序列化rpc响应并发送回客户端
// 注意：本函数现在是在**工作线程**上被调用的（handler 在池里执行并调用 done->Run()），
// 因此不能直接操作连接：
//   - TcpConnection::send() 本身线程安全（muduo 内部会 runInLoop marshal，且整段消息在
//     sendInLoop 里一次性追加到 outputBuffer_，不会与其它响应交错）；
//   - 但 TcpConnection::shutdown() 在 muduo 头文件里被明确标注
//     "NOT thread safe, no simultaneous calling"，且 TcpConnection::state_ 是非原子成员，
//     工作线程直接调用会与 loop 线程的 handleClose / 空闲定时器 / onConnection 竞争。
// 故把 send + shutdown 作为一整段 marshal 回该连接所属的 loop 执行。
void RpcProvider::sendRpcResponse(muduo::net::TcpConnectionPtr conn, const google::protobuf::Message* response)
{
    // rpc响应序列化
    std::string responseStr;
    if (!response->SerializeToString(&responseStr)) {
        LOG_ERROR("failed to serialize to string ! content:%s", responseStr.c_str());
        delete response;   // 序列化失败也需释放
        conn->getLoop()->runInLoop([conn]() { conn->shutdown(); });
        return;
    }

    // 响应加 4 字节长度前缀，客户端按长度读（长连接复用）
    int32_t respSize = responseStr.size();
    std::string sendStr;
    sendStr.append((char*)&respSize, 4);
    sendStr.append(responseStr);

    // 释放 response，避免批量大响应（批量下载）泄漏（此后不再需要它）
    delete response;

    // send + shutdown 必须在连接所属 loop 上**整段**执行，见函数上方说明。
    // 用移动初始化捕获，避免把整个响应体再拷一份。
    conn->getLoop()->runInLoop([conn, sendStr = std::move(sendStr)]() {
        conn->send(sendStr);
        // 响应后关闭连接（短连接），避免服务器累积空闲连接
        conn->shutdown();
    });
}