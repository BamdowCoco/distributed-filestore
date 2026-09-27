#include "meta_service.h"
#include "mprpc_application.h"
#include "mprpc_provider.h"

int main(int argc, char** argv)
{
    // 框架初始化
    MprpcApplication::init(argc, argv);

    // 用指针持有服务对象：run() 返回（= 收到 SIGINT/SIGTERM）后必须**先**停掉它的后台线程，
    // 再销毁它本身——顺序反了就是后台线程访问已析构的成员。
    MetaService* service = new MetaService();

    // 注册元数据服务并启动；收到 SIGINT/SIGTERM 后 run() 会返回
    RpcProvider provider;
    provider.notifyService(service);
    provider.run();

    // 停后台线程并 join（stop() 幂等，~MetaService 里还会再兜一次）
    service->stop();
    delete service;

    return 0;
}
