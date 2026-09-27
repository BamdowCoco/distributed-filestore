#include "mprpc_application.h"
#include "logger.h"

#include <cstdlib>
#include <iostream>
#include <signal.h>
#include <string>
#include <unistd.h>

MprpcConfig MprpcApplication::m_config;
bool MprpcApplication::m_hasConfigured = false;

namespace {

// 屏蔽 SIGINT/SIGTERM，把处置权交给 RpcProvider 的 signalfd（见 mprpc_provider.cc）。
//
// **必须是本进程里最早的一件事**：信号掩码是**线程创建时继承**的，而 Logger 的写线程、
// 元数据服务的 5 个后台线程都在后面才创建。只要还有任何一个线程没屏蔽它，Ctrl+C 就可能
// 被投递给那个线程并按**默认处置**终止进程——于是 signalfd 永远收不到信号，优雅退出失效
// （表现为临时节点仍要等 ZK 会话超时才消失）。这是把阻塞放在 init() 而不是 run() 里的唯一原因。
//
// 副作用：客户端（fs_caller / example caller）也走这个 init，因此 Ctrl+C 对它不再生效。
// 想中断卡住的客户端请用 Ctrl+\（SIGQUIT 未屏蔽，默认会终止）或 kill。客户端本就是
// 一次调用即退出，这个取舍可以接受；而"服务端退出时立刻摘掉 ZK 临时节点"是刚性需求。
void blockShutdownSignals()
{
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &set, nullptr);
}

}  // namespace

void showArgsHelp()
{
    std::cout << "format: command -i <configfile>" << std::endl;
}

void MprpcApplication::init(int argc, char** argv)
{
    // 必须早于 Logger::getInstance()（它会起写线程），见函数说明
    blockShutdownSignals();

    Logger::getInstance();

    if(m_hasConfigured) {
        // 只需调用一次 防止多次调用
        LOG_INFO("MprpcApplication has configured!");
        return;
    }

    m_hasConfigured = true;

    if (argc<2) {
        showArgsHelp();
        exit(EXIT_FAILURE);
    }

    int opt;
    std::string configFile;
    bool hasConfig = false;
    while ((opt = getopt(argc, argv, "+i:"))!=-1) {
        switch (opt) {
        case 'i':
            configFile = optarg;
            hasConfig = true;
            break;
        case '?':
            showArgsHelp();
            exit(EXIT_FAILURE);
        default:
            break;
        }
    }

    // 检查是否有 -i 选项
    if(!hasConfig) {
        std::cerr << argv[0] << ": without option -- 'i'" << std::endl;
        showArgsHelp();
        exit(EXIT_FAILURE);
    }    

    // 加载配置文件
    // rpc_server_ip rpc_server_port zookeeper_ip zookeeper_port
    if(!m_config.loadConfigFile(configFile))
    {
        std::cerr << "failed to load configfile!" << std::endl;
        exit(EXIT_FAILURE);
    }

    // LOG_INFO("rpc_server_ip: %s", m_config.load("rpc_server_ip").c_str());
    // LOG_INFO("rpc_server_port: %s", m_config.load("rpc_server_port").c_str());
    // LOG_INFO("zookeeper_ip: %s", m_config.load("zookeeper_ip").c_str());
    // LOG_INFO("zookeeper_port: %s", m_config.load("zookeeper_port").c_str());
    
    // LOG("rpc_server_ip:"+m_config.load("rpc_server_ip"));
    // LOG("rpc_server_port:"+m_config.load("rpc_server_port"));
    // LOG("zookeeper_ip:"+m_config.load("zookeeper_ip"));
    // LOG("zookeeper_port:"+m_config.load("zookeeper_port"));
}

MprpcApplication& MprpcApplication::getInstance()
{
    static MprpcApplication app;
    return app;
}

const MprpcConfig& MprpcApplication::getConfig()
{
    return m_config;
}