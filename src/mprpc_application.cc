#include "mprpc_application.h"
#include "logger.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>

MprpcConfig MprpcApplication::m_config;
bool MprpcApplication::m_hasConfigured = false;

void showArgsHelp()
{
    std::cout << "format: command -i <configfile>" << std::endl;
}

void MprpcApplication::init(int argc, char** argv)
{
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