#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <thread>
#include <vector>
#include <queue>

#include "lock_queue.h"

// 固定大小的工作线程池。
//
// 用途：把业务 handler 从 muduo 的 I/O 线程挪到独立线程。原先 handler 在 I/O 线程上同步执行，
// 一次慢查询 / 一个大块的 MD5 / 一次磁盘 IO 会阻塞该 loop 上**所有**连接（head-of-line blocking）。
//
// 任务类型是 std::function<void()>，池本身不关心任务内容——RPC 请求相关的资源
// （request/response/closure）由提交方用 RAII 持有，见 mprpc_provider.cc 的 RpcTask。
class ThreadPool
{
public:
    ThreadPool() = default;
    ~ThreadPool() { stop(); }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // 启动 n 个 worker；n <= 0 或已在运行时不做事
    void start(int n)
    {
        if (n <= 0 || m_running.exchange(true)) {
            return;
        }
        m_workers.reserve(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i) {
            m_workers.emplace_back([this]() { workerLoop(); });
        }
    }

    // 提交任务。返回 false 表示未启动或已达上限 maxPending，
    // 此时任务**不会**被执行，其资源由任务自身的 RAII 在析构时释放。
    //
    // 上限判断是无锁的近似：并发提交最多超出 (提交方线程数 - 1)。这是有意的——
    // 它只是一道背压护栏（防止排队深度 × 单请求 64MB 无界增长），
    // 精确控制交给上层（本框架里是「每个连接最多一个在途请求」）。
    bool submit(std::function<void()> task)
    {
        if (!m_running.load()) {
            return false;
        }
        if (m_pending.load() >= m_maxPending) {
            return false;
        }
        m_pending.fetch_add(1);
        m_queue.push(std::move(task));
        return true;
    }

    // 停止：唤醒全部 worker 并 join；随后丢弃队列中未执行的任务
    // （其资源由任务自身 RAII 释放，不会泄漏）。
    //
    // 注意 LockQueue::pop() 没有停止谓词，只会在队列非空时返回，因此
    // 「设标志再 join」会永久挂死——必须靠哨兵任务把 worker 叫醒。
    void stop()
    {
        if (!m_running.exchange(false)) {
            return;   // 未启动或已停过
        }
        for (size_t i = 0; i < m_workers.size(); ++i) {
            m_queue.push(std::function<void()>());   // 空任务 = 退出信号
        }
        for (auto& t : m_workers) {
            if (t.joinable()) {
                t.join();
            }
        }
        m_workers.clear();

        // 丢弃剩余任务：swap 出来后离开作用域即逐个析构
        std::queue<std::function<void()>> leftover;
        m_queue.drain(leftover);
        m_pending.store(0);
    }

    // 已提交但尚未执行完成的任务数（**含队列中与正在执行的**）——
    // 用它做背压上限比只算队列长度更准确。
    size_t pending() const { return m_pending.load(); }

    // 设置队列上限（在 start 之前调用）
    void setMaxPending(size_t maxPending) { m_maxPending = maxPending; }

    bool running() const { return m_running.load(); }

private:
    void workerLoop()
    {
        while (true) {
            std::function<void()> task = m_queue.pop();
            if (!task) {
                break;   // 哨兵
            }
            task();
            m_pending.fetch_sub(1);
        }
    }

    LockQueue<std::function<void()>> m_queue;
    std::vector<std::thread> m_workers;
    std::atomic<bool> m_running{false};
    std::atomic<size_t> m_pending{0};
    size_t m_maxPending = 1024;
};
