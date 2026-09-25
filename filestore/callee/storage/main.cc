#include "mprpc_application.h"
#include "mprpc_provider.h"
#include "storage_service.h"

int main(int argc, char** argv)
{
    // 框架初始化
    MprpcApplication::init(argc, argv);

    // 注册存储服务并启动
    RpcProvider provider;
    provider.notifyService(new StorageService());
    provider.run();

    return 0;
}
