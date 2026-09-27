#include "user_dao.h"

#include "sql.h"

bool UserDao::findByName(const std::string& username, Row& out) const
{
    MYSQL_RES* res = m_conn.query(
        "SELECT id, password_hash FROM user WHERE username=" +
        Sql::str(m_conn.getConn(), username));
    if (res == nullptr) {
        return false;
    }
    bool found = false;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row != nullptr) {
        found = colInt(row, 0, out.id);
        out.passwordHash = colStr(row, 1);   // 必须在 mysql_free_result 之前拷走
    }
    mysql_free_result(res);
    return found;
}

bool UserDao::existsName(const std::string& username) const
{
    MYSQL_RES* res = m_conn.query(
        "SELECT id FROM user WHERE username=" + Sql::str(m_conn.getConn(), username));
    if (res == nullptr) {
        return false;
    }
    bool exists = mysql_num_rows(res) > 0;
    mysql_free_result(res);
    return exists;
}

bool UserDao::insert(const std::string& username, const std::string& passwordHash)
{
    return m_conn.update(
        "INSERT INTO user(username, password_hash) VALUES(" +
        Sql::str(m_conn.getConn(), username) + ", " +
        Sql::str(m_conn.getConn(), passwordHash) + ")");
}

bool UserDao::updatePasswordHash(int userId, const std::string& passwordHash)
{
    return m_conn.update(
        "UPDATE user SET password_hash=" + Sql::str(m_conn.getConn(), passwordHash) +
        " WHERE id=" + Sql::num(userId));
}
