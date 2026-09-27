#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../Connection.hpp"

// cleanup_queue 表：待清理的孤儿块任务（删块失败后入队，指数退避重试）。
// status: 0=待清理 1=已完成 3=需人工
class CleanupQueueDao
{
public:
    struct Task
    {
        int id = 0;
        std::string ip;
        int port = 0;
        int fileId = 0;
        int retryCount = 0;
    };

    explicit CleanupQueueDao(Connection& conn) : m_conn(conn) {}

    // 幂等入队：同 (ip,port,fileId) 已存在则重置为待清理
    bool upsert(const std::string& ip, int port, int fileId);

    // 取该任务的 id（供推 Redis Stream 用）
    bool findIdByKey(const std::string& ip, int port, int fileId, int& outId) const;

    // 取「到期且待清理」的单个任务；不存在或未到期返回 false
    bool findDueById(int id, Task& out) const;

    // 列出所有「到期且待清理」的任务 id
    bool listDueIds(std::vector<int>& out) const;

    // 队列在管的、落在 [lo, hi) 内的 file_id（分区间对账用）
    bool listFileIdsInRange(int lo, int hi, std::vector<int>& out) const;

    bool markDone(int id);

    // 重试超限，进入终态（需人工处理）
    bool markManual(int id);

    // 指数退避：backoffSec 秒后重试
    bool markRetryBackoff(int id, int backoffSec);

private:
    Connection& m_conn;
};
