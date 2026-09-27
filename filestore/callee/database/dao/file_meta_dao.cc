#include "file_meta_dao.h"

#include "sql.h"

bool FileMetaDao::insertPending(int64_t filesize, int chunkCount, int& outFileId)
{
    if (!m_conn.update(
            "INSERT INTO file_meta(filesize, chunk_count, status) VALUES(" +
            Sql::num(filesize) + ", " + Sql::num(chunkCount) + ", 0)")) {
        return false;
    }
    // 取本次分配的自增 file_id：LAST_INSERT_ID() 是**连接（会话）级**的，
    // 必须紧接着在同一条连接上查（调用方在事务中，连接不会被切走）。
    MYSQL_RES* res = m_conn.query("SELECT LAST_INSERT_ID()");
    if (res == nullptr) {
        return false;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    bool ok = colInt(row, 0, outFileId) && outFileId > 0;
    mysql_free_result(res);
    return ok;
}

bool FileMetaDao::lockOwnedStatus(int fileId, int userId, int& outStatus, std::string& reason) const
{
    MYSQL_RES* res = m_conn.query(
        "SELECT fm.status FROM file_meta fm JOIN file_node fn ON fn.file_id = fm.id "
        "WHERE fm.id=" + Sql::num(fileId) + " AND fn.user_id=" + Sql::num(userId) + " FOR UPDATE");
    if (res == nullptr) {
        reason = "query failed";
        return false;
    }
    bool found = false;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row != nullptr && colInt(row, 0, outStatus)) {
        found = true;
    }
    mysql_free_result(res);
    if (!found) {
        // 不区分"不存在"与"不属于当前用户"，避免泄露他人 file_id 是否存在
        reason = "file not found or permission denied";
    }
    return found;
}

bool FileMetaDao::findComplete(int fileId, int64_t& outFilesize, int& outChunkCount) const
{
    MYSQL_RES* res = m_conn.query(
        "SELECT filesize, chunk_count FROM file_meta WHERE id=" + Sql::num(fileId) +
        " AND status=1");
    if (res == nullptr) {
        return false;
    }
    bool ok = false;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row != nullptr) {
        ok = colInt64(row, 0, outFilesize) && colInt(row, 1, outChunkCount);
    }
    mysql_free_result(res);
    return ok;
}

bool FileMetaDao::updateStatusComplete(int fileId)
{
    return m_conn.update("UPDATE file_meta SET status=1 WHERE id=" + Sql::num(fileId));
}

bool FileMetaDao::deleteById(int fileId)
{
    return m_conn.update("DELETE FROM file_meta WHERE id=" + Sql::num(fileId));
}

bool FileMetaDao::listIdsInRange(int lo, int hi, std::vector<int>& out) const
{
    out.clear();
    MYSQL_RES* res = m_conn.query(
        "SELECT id FROM file_meta WHERE id >= " + Sql::num(lo) + " AND id < " + Sql::num(hi));
    if (res == nullptr) {
        return false;
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        int id = 0;
        if (colInt(row, 0, id)) {
            out.push_back(id);
        }
    }
    mysql_free_result(res);
    return true;
}

bool FileMetaDao::maxId(int& out) const
{
    MYSQL_RES* res = m_conn.query("SELECT MAX(id) FROM file_meta");
    if (res == nullptr) {
        return false;
    }
    bool ok = false;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row != nullptr) {
        if (row[0] == nullptr) {
            out = 0;   // 空表：MAX 为 NULL
            ok = true;
        } else {
            ok = colInt(row, 0, out);
        }
    }
    mysql_free_result(res);
    return ok;
}

bool FileMetaDao::listStalePendingIds(int ttlMinutes, std::vector<int>& out) const
{
    out.clear();
    MYSQL_RES* res = m_conn.query(
        "SELECT id FROM file_meta WHERE status=0 AND created_at < NOW() - INTERVAL " +
        Sql::num(ttlMinutes) + " MINUTE");
    if (res == nullptr) {
        return false;
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        int id = 0;
        if (colInt(row, 0, id)) {
            out.push_back(id);
        }
    }
    mysql_free_result(res);
    return true;
}
