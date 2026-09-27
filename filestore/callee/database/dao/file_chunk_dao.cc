#include "file_chunk_dao.h"

#include "sql.h"

bool FileChunkDao::insertMany(int fileId, const std::vector<Loc>& locs)
{
    if (locs.empty()) {
        return true;   // 空文件没有块，无需执行语句（避免拼出非法的空 VALUES）
    }
    std::string sql = "INSERT INTO file_chunk(file_id, chunk_index, ip, port) VALUES";
    for (size_t i = 0; i < locs.size(); ++i) {
        if (i > 0) {
            sql += ",";
        }
        sql += "(" + Sql::num(fileId) + ", " + Sql::num(locs[i].chunkIndex) + ", " +
               Sql::str(m_conn.getConn(), locs[i].ip) + ", " + Sql::num(locs[i].port) + ")";
    }
    return m_conn.update(sql);
}

bool FileChunkDao::updateChunkMeta(int fileId, int chunkIndex, const std::string& checksum,
                                   int64_t offset, int size)
{
    if (!m_conn.update(
            "UPDATE file_chunk SET checksum=" + Sql::str(m_conn.getConn(), checksum) +
            ", offset=" + Sql::num(offset) + ", size=" + Sql::num(size) +
            " WHERE file_id=" + Sql::num(fileId) + " AND chunk_index=" + Sql::num(chunkIndex))) {
        return false;
    }
    // 调用方（CommitUpload）已核对过块记录数齐全，因此 0 行只可能是 chunk_index 越界。
    // 这个判断原先写在 handler 里（conn->affectedRows()），收进 DAO 以免业务侧再摸连接句柄。
    return m_conn.affectedRows() > 0;
}

bool FileChunkDao::countByFile(int fileId, int& out) const
{
    MYSQL_RES* res = m_conn.query(
        "SELECT COUNT(*) FROM file_chunk WHERE file_id=" + Sql::num(fileId));
    if (res == nullptr) {
        return false;
    }
    bool ok = false;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row != nullptr) {
        ok = colInt(row, 0, out);
    }
    mysql_free_result(res);
    return ok;
}

bool FileChunkDao::listByFile(int fileId, std::vector<Row>& out) const
{
    out.clear();
    MYSQL_RES* res = m_conn.query(
        "SELECT chunk_index, ip, port, checksum, offset, size FROM file_chunk WHERE file_id=" +
        Sql::num(fileId) + " ORDER BY chunk_index");
    if (res == nullptr) {
        return false;
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        Row r;
        int idx = 0;
        if (!colInt(row, 0, idx)) {
            continue;
        }
        r.chunkIndex = idx;
        r.ip = colStr(row, 1);          // 字符串列必须在 free 之前拷走
        int port = 0;
        colInt(row, 2, port);
        r.port = port;
        r.checksum = colStr(row, 3);
        colInt64(row, 4, r.offset);
        int size = 0;
        colInt(row, 5, size);
        r.size = size;
        out.push_back(r);
    }
    mysql_free_result(res);
    return true;
}

bool FileChunkDao::distinctNodes(int fileId, std::vector<NodeAddr>& out) const
{
    out.clear();
    MYSQL_RES* res = m_conn.query(
        "SELECT DISTINCT ip, port FROM file_chunk WHERE file_id=" + Sql::num(fileId));
    if (res == nullptr) {
        return false;
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        NodeAddr n;
        n.ip = colStr(row, 0);
        int port = 0;
        if (colInt(row, 1, port)) {
            n.port = port;
        }
        if (!n.ip.empty()) {
            out.push_back(n);
        }
    }
    mysql_free_result(res);
    return true;
}

bool FileChunkDao::deleteByFile(int fileId)
{
    return m_conn.update("DELETE FROM file_chunk WHERE file_id=" + Sql::num(fileId));
}
