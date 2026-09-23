// RPC 调用延迟基准：直连 FriendService（bin/callee，端口 8000）循环调用 GetFriendList，
// 测量单次 RPC 往返延迟。口径：短连接建连 + 请求序列化 + 网络往返 + 响应反序列化
// （含 TCP 三次握手/四次挥手，因为框架直连模式已改为短连接）。
//
// 用法：bench_rpc_latency [次数]
// 结果输出到 stderr（stdout 被重定向以屏蔽框架 LOG_INFO 的同步打印，避免污染延迟测量）。

#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "friend.pb.h"

#include <chrono>
#include <vector>
#include <algorithm>
#include <cstdio>
#include <cstdlib>

int main(int argc, char** argv)
{
    int N = 10000;
    if (argc > 1) {
        N = std::atoi(argv[1]);
    }
    if (N <= 0) {
        N = 10000;
    }

    // 屏蔽框架 LOG_INFO 的 stdout 打印（同步 I/O + endl flush 会主导延迟），结果走 stderr
    freopen("/dev/null", "w", stdout);

    MprpcChannel channel("127.0.0.1", 8000);
    fixbug::FriendRpcService_Stub stub(&channel);

    // 预热：触发懒初始化、TCP 端口就绪，避免首包抖动计入样本
    for (int i = 0; i < 100; ++i) {
        fixbug::GetFriendListRequest req;
        fixbug::GetFriendListResponse resp;
        req.set_id(i);
        MprpcController ctl;
        stub.GetFriendList(&ctl, &req, &resp, nullptr);
    }

    std::vector<double> lat_us;
    lat_us.reserve(N);
    int failed = 0;

    for (int i = 0; i < N; ++i) {
        fixbug::GetFriendListRequest req;
        fixbug::GetFriendListResponse resp;
        req.set_id(i);
        MprpcController ctl;

        auto t0 = std::chrono::steady_clock::now();
        stub.GetFriendList(&ctl, &req, &resp, nullptr);
        auto t1 = std::chrono::steady_clock::now();

        if (ctl.Failed()) {
            ++failed;
            std::fprintf(stderr, "rpc failed at %d: %s\n", i, ctl.ErrorText().c_str());
            continue;
        }
        lat_us.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }

    if (lat_us.empty()) {
        std::fprintf(stderr, "RPC_LATENCY no successful call\n");
        return 1;
    }

    std::sort(lat_us.begin(), lat_us.end());
    double sum = 0.0;
    for (double v : lat_us) {
        sum += v;
    }
    double avg = sum / static_cast<double>(lat_us.size());
    double median = lat_us[lat_us.size() / 2];
    double p99 = lat_us[static_cast<size_t>(lat_us.size() * 0.99)];

    // 结果走 stderr，run_new_tests.sh 从这里解析
    std::fprintf(stderr,
                 "RPC_LATENCY samples=%zu failed=%d avg=%.2fus median=%.2fus p99=%.2fus\n",
                 lat_us.size(), failed, avg, median, p99);

    return 0;
}
