#pragma once

#include "../Connection.hpp"

// RAII 事务守卫。
//
// 取代原先散落在各处的 "START TRANSACTION; ... COMMIT / ROLLBACK"：手写时共有
// 4 个 START、4 个 COMMIT 与 **14 处手写 ROLLBACK**，只要有一条提前 return 漏了回滚，
// 就会把仍挂着事务的连接还回连接池，污染后续所有请求（连接池不做 ping，也不重置会话）。
//
// 用法：
//     Transaction tx(*conn);
//     if (!tx.ok()) { /* 报错返回 */ }
//     ...DAO 调用（都传同一个 conn，事务才连成一片）...
//     if (!tx.commit()) { /* 报错返回；提交失败已自动回滚 */ }
//     // 未 commit 就离开作用域 → 析构时自动 ROLLBACK
class Transaction
{
public:
    explicit Transaction(Connection& conn)
        : m_conn(conn), m_active(conn.update("START TRANSACTION"))
    {
    }

    ~Transaction()
    {
        if (m_active) {
            // 走到这里说明调用方没提交就离开了作用域（提前 return / 抛异常）——
            // 必须回滚，绝不能把事务留给连接池里的下一个使用者。
            m_conn.update("ROLLBACK");
        }
    }

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;

    // 事务是否已成功开启
    bool ok() const { return m_active; }

    // 提交；返回 false 表示提交失败（此时已显式回滚）
    bool commit()
    {
        if (!m_active) {
            return false;
        }
        bool committed = m_conn.update("COMMIT");
        m_active = false;   // 无论成败，事务都已结束（失败时下面再补一次回滚）
        if (!committed) {
            m_conn.update("ROLLBACK");
        }
        return committed;
    }

private:
    Connection& m_conn;
    bool m_active = false;
};
