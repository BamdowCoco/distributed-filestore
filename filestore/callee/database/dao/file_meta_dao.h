#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../Connection.hpp"

// file_meta 表：每个文件一行，主键 id 即 file_id（数据文件以它命名）。
class FileMetaDao
{
public:
    explicit FileMetaDao(Connection& conn) : m_conn(conn) {}

    // 插入 PENDING 记录并返回分配到的 file_id（返回 false 表示失败）
    bool insertPending(int64_t filesize, int chunkCount, int& outFileId);

    // 事务内锁定该文件行并校验归属；成功输出 status（0=PENDING / 1=COMPLETE）。
    // **必须在事务内调用**（FOR UPDATE 依赖事务持有行锁）；失败时 reason 说明原因。
    // 归属关系在 file_node.user_id（file_meta 本身没有 user_id 列）。
    bool lockOwnedStatus(int fileId, int userId, int& outStatus, std::string& reason) const;

    // 取 COMPLETE 文件的大小与块数；不存在或非 COMPLETE 返回 false
    bool findComplete(int fileId, int64_t& outFilesize, int& outChunkCount) const;

    // PENDING -> COMPLETE
    bool updateStatusComplete(int fileId);

    bool deleteById(int fileId);

    // 主键区间 [lo, hi) 内的 file_id（分区间对账用；主键上有索引，页内有界）
    bool listIdsInRange(int lo, int hi, std::vector<int>& out) const;

    // 当前最大 file_id（0 表示表为空）。主键 MAX 是 O(1)，用来判断对账游标是否已扫到尾部。
    bool maxId(int& out) const;

    // 超过 ttlMinutes 仍为 PENDING 的 file_id（客户端上传中途崩溃的残留）
    bool listStalePendingIds(int ttlMinutes, std::vector<int>& out) const;

private:
    Connection& m_conn;
};
