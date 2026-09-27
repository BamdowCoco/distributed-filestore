#pragma once

#include <string>

#include "../Connection.hpp"

// user 表（账号）。
//
// 约定：DAO 在构造时接收调用方的 Connection&，方法内部只用它——
// DAO 自己不去连接池取连接，否则一个业务操作里的多条语句会落在不同连接上、事务就散了。
class UserDao
{
public:
    struct Row
    {
        int id = 0;
        std::string passwordHash;
    };

    explicit UserDao(Connection& conn) : m_conn(conn) {}

    // 按用户名取 id 与口令散列；不存在返回 false
    bool findByName(const std::string& username, Row& out) const;

    // 用户名是否已存在
    bool existsName(const std::string& username) const;

    // 插入新账号
    bool insert(const std::string& username, const std::string& passwordHash);

    // 更新口令散列（用于旧格式登录后自动升级）
    bool updatePasswordHash(int userId, const std::string& passwordHash);

private:
    Connection& m_conn;
};
