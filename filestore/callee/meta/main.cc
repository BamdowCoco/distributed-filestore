#include "meta_service.h"
#include "mprpc_application.h"
#include "mprpc_provider.h"

int main(int argc, char** argv)
{
    // 框架初始化
    MprpcApplication::init(argc, argv);

    // 注册元数据服务并启动
    RpcProvider provider;
    provider.notifyService(new MetaService());
    provider.run();

    return 0;
}
