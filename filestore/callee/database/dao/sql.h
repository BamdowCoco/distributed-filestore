#pragma once

#include <cstdint>
#include <string>

#include <mysql/mysql.h>

#include "common/common.h"   // parseNonNegativeInt / parseNonNegativeInt64

// SQL 片段构造的**唯一入口**。
//
// 为什么要有这一层：业务代码里手写 SQL 并手工调 escapeSql 的写法已经出过事——
// `Register` 里把 escapeSql 的结果算出来却忘了用，INSERT 用的是未转义值。
// 把字符串拼接收敛到 DAO 内部、并让转义成为**唯一可用的构造方式**，
// 就能从结构上消除"忘记转义"这类漏洞（而不是靠人记得）。
//
// 用法：拼 SQL 时所有字符串都必须经 Sql::str()，数值经 Sql::num()。
class Sql
{
public:
    // 字符串字面量：自动经 mysql_real_escape_string 转义后加上单引号
    static std::string str(MYSQL* conn, const std::string& value)
    {
        return "'" + escaped(conn, value) + "'";
    }

    // 数值字面量
    static std::string num(int64_t value) { return std::to_string(value); }

    // 裸的转义结果（仅在确实需要不带引号时使用，例如拼 LIKE 模式串）
    static std::string escaped(MYSQL* conn, const std::string& value)
    {
        if (conn == nullptr) {
            return std::string();   // 无连接时返回空串而不是原文——宁可报错也不要放未转义的内容出去
        }
        // mysql_real_escape_string 最坏情况每个字节膨胀为两字节（\、'、"、NUL 等），
        // 故按 2 倍 + 1 预留；它不做 NUL 截断，返回值给出真实长度。
        std::string buf(value.size() * 2 + 1, '\0');
        unsigned long len = mysql_real_escape_string(conn, &buf[0], value.c_str(), value.size());
        buf.resize(len);
        return buf;
    }
};

// ---------------------------------------------------------------------------
// 结果集读取小工具：把 mysql_fetch_row 的两个坑收敛到一处
//   1) 列可能为 NULL（必须判空）；
//   2) 字符串列必须在 mysql_free_result **之前**拷成 std::string，否则是悬垂指针。
// colStr 直接返回 std::string，使"拷走"成为默认行为。
// ---------------------------------------------------------------------------

// 读取非 NULL 的 int 列；列不存在或非数字返回 false（不抛异常）
inline bool colInt(MYSQL_ROW row, unsigned idx, int& out)
{
    if (row == nullptr || row[idx] == nullptr) {
        return false;
    }
    return parseNonNegativeInt(row[idx], out);
}

// 读取非 NULL 的 int64 列
inline bool colInt64(MYSQL_ROW row, unsigned idx, int64_t& out)
{
    if (row == nullptr || row[idx] == nullptr) {
        return false;
    }
    return parseNonNegativeInt64(row[idx], out);
}

// 读取字符串列（NULL 视为空串）
inline std::string colStr(MYSQL_ROW row, unsigned idx)
{
    if (row == nullptr || row[idx] == nullptr) {
        return std::string();
    }
    return std::string(row[idx]);
}
