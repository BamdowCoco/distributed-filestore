#include "cleanup_queue_dao.h"

#include "sql.h"

bool CleanupQueueDao::upsert(const std::string& ip, int port, int fileId)
{
    std::string ipEsc = Sql::str(m_conn.getConn(), ip);
    return m_conn.update(
        "INSERT INTO cleanup_queue(node_ip, node_port, file_id, status, retry_count, next_retry_at) "
        "VALUES(" + ipEsc + ", " + Sql::num(port) + ", " + Sql::num(fileId) + ", 0, 0, NOW()) "
        "ON DUPLICATE KEY UPDATE status=0, retry_count=0, next_retry_at=NOW()");
}

bool CleanupQueueDao::findIdByKey(const std::string& ip, int port, int fileId, int& outId) const
{
    MYSQL_RES* res = m_conn.query(
        "SELECT id FROM cleanup_queue WHERE node_ip=" + Sql::str(m_conn.getConn(), ip) +
        " AND node_port=" + Sql::num(port) + " AND file_id=" + Sql::num(fileId));
    if (res == nullptr) {
        return false;
    }
    bool found = false;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row != nullptr) {
        found = colInt(row, 0, outId);
    }
    mysql_free_result(res);
    return found;
}

bool CleanupQueueDao::findDueById(int id, Task& out) const
{
    MYSQL_RES* res = m_conn.query(
        "SELECT node_ip, node_port, file_id, retry_count FROM cleanup_queue WHERE id=" +
        Sql::num(id) + " AND status=0 AND next_retry_at <= NOW()");
    if (res == nullptr) {
        return false;
    }
    bool found = false;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row != nullptr) {
        out.ip = colStr(row, 0);   // 必须在 free 之前拷走
        int port = 0;
        int fileId = 0;
        int retry = 0;
        colInt(row, 1, port);
        colInt(row, 2, fileId);
        colInt(row, 3, retry);
        out.port = port;
        out.fileId = fileId;
        out.retryCount = retry;
        out.id = id;
        found = !out.ip.empty();
    }
    mysql_free_result(res);
    return found;
}

bool CleanupQueueDao::listDueIds(std::vector<int>& out) const
{
    out.clear();
    MYSQL_RES* res = m_conn.query(
        "SELECT id FROM cleanup_queue WHERE status=0 AND next_retry_at <= NOW()");
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

bool CleanupQueueDao::listAllFileIds(std::vector<int>& out) const
{
    out.clear();
    MYSQL_RES* res = m_conn.query("SELECT file_id FROM cleanup_queue");
    if (res == nullptr) {
        return false;
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        int fileId = 0;
        if (colInt(row, 0, fileId)) {
            out.push_back(fileId);
        }
    }
    mysql_free_result(res);
    return true;
}

bool CleanupQueueDao::listFileIdsInRange(int lo, int hi, std::vector<int>& out) const
{
    out.clear();
    // file_id 上需要 KEY idx_file_id，否则该区间查询退化为全表扫描（见 Schema::ensure）
    MYSQL_RES* res = m_conn.query(
        "SELECT file_id FROM cleanup_queue WHERE file_id >= " + Sql::num(lo) + " AND file_id < " +
        Sql::num(hi));
    if (res == nullptr) {
        return false;
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        int fileId = 0;
        if (colInt(row, 0, fileId)) {
            out.push_back(fileId);
        }
    }
    mysql_free_result(res);
    return true;
}

bool CleanupQueueDao::markDone(int id)
{
    return m_conn.update("UPDATE cleanup_queue SET status=1 WHERE id=" + Sql::num(id));
}

bool CleanupQueueDao::markManual(int id)
{
    return m_conn.update(
        "UPDATE cleanup_queue SET status=3, retry_count=retry_count+1 WHERE id=" + Sql::num(id));
}

bool CleanupQueueDao::markRetryBackoff(int id, int backoffSec)
{
    return m_conn.update(
        "UPDATE cleanup_queue SET retry_count=retry_count+1, "
        "next_retry_at=DATE_ADD(NOW(), INTERVAL " + Sql::num(backoffSec) + " SECOND) WHERE id=" +
        Sql::num(id));
}
