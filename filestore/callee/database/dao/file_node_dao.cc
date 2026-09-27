#include "file_node_dao.h"

#include "sql.h"

bool FileNodeDao::findChild(int userId, int parentId, const std::string& name, Row& out) const
{
    // resolvePath / nodeExists / resolveParentDir 原先各写了一条形状相同的查询，这里合并
    MYSQL_RES* res = m_conn.query(
        "SELECT id, name, is_dir, COALESCE(file_id,0) FROM file_node WHERE user_id=" +
        Sql::num(userId) + " AND parent_id=" + Sql::num(parentId) +
        " AND name=" + Sql::str(m_conn.getConn(), name));
    if (res == nullptr) {
        return false;
    }
    bool found = false;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row != nullptr && colInt(row, 0, out.id)) {
        out.name = colStr(row, 1);   // 必须在 free 之前拷走
        int isDir = 0;
        int fileId = 0;
        colInt(row, 2, isDir);
        colInt(row, 3, fileId);
        out.isDir = (isDir != 0);
        out.fileId = fileId;
        found = true;
    }
    mysql_free_result(res);
    return found;
}

bool FileNodeDao::findChildDirId(int userId, int parentId, const std::string& name, int& outId) const
{
    MYSQL_RES* res = m_conn.query(
        "SELECT id FROM file_node WHERE user_id=" + Sql::num(userId) +
        " AND parent_id=" + Sql::num(parentId) + " AND name=" +
        Sql::str(m_conn.getConn(), name) + " AND is_dir=1");
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

bool FileNodeDao::insert(int userId, int parentId, const std::string& name, bool isDir,
                         int fileIdOrZero)
{
    // 目录的 file_id 必须写 NULL（列可空）；0 是合法的自增值起点，故用 <=0 表示"无"
    std::string fileIdSql = (fileIdOrZero > 0) ? Sql::num(fileIdOrZero) : "NULL";
    return m_conn.update(
        "INSERT INTO file_node(user_id, parent_id, name, is_dir, file_id) VALUES(" +
        Sql::num(userId) + ", " + Sql::num(parentId) + ", " +
        Sql::str(m_conn.getConn(), name) + ", " + (isDir ? "1" : "0") + ", " + fileIdSql + ")");
}

bool FileNodeDao::listChildren(int userId, int parentId, std::vector<Row>& out) const
{
    out.clear();
    MYSQL_RES* res = m_conn.query(
        "SELECT id, name, is_dir, COALESCE(file_id,0) FROM file_node WHERE user_id=" +
        Sql::num(userId) + " AND parent_id=" + Sql::num(parentId) + " ORDER BY name");
    if (res == nullptr) {
        return false;
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        Row r;
        if (!colInt(row, 0, r.id)) {
            continue;
        }
        r.name = colStr(row, 1);
        int isDir = 0;
        int fileId = 0;
        colInt(row, 2, isDir);
        colInt(row, 3, fileId);
        r.isDir = (isDir != 0);
        r.fileId = fileId;
        out.push_back(r);
    }
    mysql_free_result(res);
    return true;
}

bool FileNodeDao::deleteById(int nodeId)
{
    return m_conn.update("DELETE FROM file_node WHERE id=" + Sql::num(nodeId));
}

bool FileNodeDao::deleteByFileId(int fileId)
{
    return m_conn.update("DELETE FROM file_node WHERE file_id=" + Sql::num(fileId));
}
