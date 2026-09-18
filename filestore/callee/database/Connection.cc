#include "Connection.hpp"
#include "logger.h"

Connection::Connection()
{
    _conn = mysql_init(nullptr);
}

Connection::~Connection()
{
    if (_conn != nullptr) {
        mysql_close(_conn);
        _conn = nullptr;
    }
}

bool Connection::connect(std::string ip, unsigned short port,
                         std::string user, std::string password,
                         std::string dbname)
{
    if (_conn == nullptr) {
        return false;
    }

    MYSQL* ret = mysql_real_connect(_conn, ip.c_str(), user.c_str(), password.c_str(),
                                    dbname.c_str(), port, nullptr, 0);
    if (ret == nullptr) {
        LOG_ERROR("connect mysql failed! ip:%s port:%d error:%s",
                  ip.c_str(), port, mysql_error(_conn));
        return false;
    }
    return true;
}

bool Connection::update(std::string sql)
{
    if (_conn == nullptr) {
        return false;
    }
    if (mysql_query(_conn, sql.c_str()) != 0) {
        LOG_ERROR("update sql failed! sql:%s error:%s",
                  sql.c_str(), mysql_error(_conn));
        return false;
    }
    return true;
}

MYSQL_RES* Connection::query(std::string sql)
{
    if (_conn == nullptr) {
        return nullptr;
    }
    if (mysql_query(_conn, sql.c_str()) != 0) {
        LOG_ERROR("query sql failed! sql:%s error:%s",
                  sql.c_str(), mysql_error(_conn));
        return nullptr;
    }
    return mysql_store_result(_conn);
}

void Connection::refreshIdleStart()
{
    _idleStart = Clock::now();
}

double Connection::getIdleAliveDuration() const
{
    return Duration(Clock::now() - _idleStart).count();
}

MYSQL* Connection::getConn()
{
    return _conn;
}
