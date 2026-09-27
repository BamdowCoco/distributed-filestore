// ThreadPool 的单元测试（include/thread_pool.h）。不依赖 ZooKeeper / MySQL / 服务进程。
// 编译：g++ -O2 -std=c++17 -I include test_thread_pool.cc -o test_thread_pool -lpthread
//
// 重点覆盖「停止」这条路径：LockQueue::pop() 没有停止谓词，只会在队列非空时返回，
// 因此 stop() 若不用哨兵唤醒 worker 就会永久挂死。本测试卡住即代表该 bug 复现。
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <thread>

#include "thread_pool.h"

static int g_failed = 0;

static void expect(bool cond, const char* what)
{
    std::printf("  %s %s\n", cond ? "ok  " : "FAIL", what);
    if (!cond) {
        ++g_failed;
    }
}

// 用于验证「被丢弃的任务其资源被释放」：析构时计数
struct Tracker
{
    static std::atomic<int> destructed;
    int id;
    explicit Tracker(int i) : id(i) {}
    ~Tracker() { destructed.fetch_add(1); }
};
std::atomic<int> Tracker::destructed{0};

// 忙等直到条件成立或超时
static bool waitFor(const std::function<bool()>& cond, int timeoutMs)
{
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (cond()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return cond();
}

int main()
{
    std::printf("===== ThreadPool 单元测试 =====\n");

    // 1. 未启动时提交应被拒
    {
        ThreadPool pool;
        expect(!pool.submit([]() {}), "未启动时提交被拒绝");
    }

    // 2. 全部任务被执行
    {
        ThreadPool pool;
        pool.setMaxPending(100000);   // 本用例要一次性提交 2000 个，先把背压上限抬高
        pool.start(4);
        expect(pool.running(), "start 后 running() 为真");

        std::atomic<int> ran{0};
        const int N = 2000;
        for (int i = 0; i < N; ++i) {
            bool ok = pool.submit([&ran]() {
                ran.fetch_add(1);
            });
            if (!ok) {
                std::printf("  FAIL 第 %d 个任务被意外拒绝（上限过高才会发生）\n", i);
                ++g_failed;
                break;
            }
        }
        expect(waitFor([&ran]() { return ran.load() == N; }, 10000),
               "2000 个任务全部执行完毕");
        expect(pool.pending() == 0, "执行完毕后 pending() 归零");

        // 3. 停止后不再接受任务，且 stop() 不挂死（本测试能走到下一行即是证明）
        pool.stop();
        expect(!pool.running(), "stop 后 running() 为假");
        expect(!pool.submit([]() {}), "stop 后提交被拒绝");
        pool.stop();   // 重复 stop 应安全
        expect(true, "重复 stop() 不崩溃、不挂死");
    }

    // 4. 队列里仍有任务时停止：不得挂死，且被丢弃任务的资源必须被释放
    {
        ThreadPool pool;
        pool.start(1);
        // 先占住唯一的 worker，让后续任务堆在队列里
        std::atomic<bool> releaseGate{false};
        pool.submit([&releaseGate]() {
            while (!releaseGate.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });

        std::atomic<int> queued{0};
        const int M = 200;
        for (int i = 0; i < M; ++i) {
            // 每个任务持有 shared_ptr<Tracker>；任务被丢弃时 shared_ptr 析构 → Tracker 析构
            auto tracker = std::make_shared<Tracker>(i);
            pool.submit([tracker, &queued]() {
                queued.fetch_add(1);
            });
        }

        releaseGate.store(true);   // 放行 worker，随后 stop
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        pool.stop();               // 此处若挂死，说明哨兵逻辑有问题
        expect(true, "队列非空时 stop() 不挂死");

        // 被丢弃的任务（未执行的）其 shared_ptr 已释放；已执行的也会释放。
        // 两种情况合计应等于提交总数。
        expect(waitFor([]() { return Tracker::destructed.load() == M; }, 3000),
               "全部任务对象（含被丢弃的）都已释放，无资源泄漏");
    }

    // 5. 背压：达到上限后 submit 返回 false
    {
        ThreadPool pool;
        pool.setMaxPending(10);
        pool.start(1);
        std::atomic<bool> hold{false};
        pool.submit([&hold]() {   // 占住 worker
            while (!hold.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
        int accepted = 0;
        for (int i = 0; i < 100; ++i) {
            if (pool.submit([]() {})) {
                ++accepted;
            }
        }
        expect(accepted <= 11, "达到上限后 submit 开始拒绝（背压生效）");
        hold.store(true);
        pool.stop();
    }

    if (g_failed == 0) {
        std::printf("\n全部通过\n");
        return 0;
    }
    std::printf("\n失败 %d 项\n", g_failed);
    return 1;
}
