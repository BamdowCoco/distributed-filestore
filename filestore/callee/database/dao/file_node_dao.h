#pragma once

#include <string>
#include <vector>

#include "../Connection.hpp"

// file_node 表：虚拟文件树（同一 user_id + parent_id 下 name 唯一）。
// 文件的归属关系就在本表的 user_id 上——file_meta 没有 user_id 列。
class FileNodeDao
{
public:
    // 一个子节点
    struct Row
    {
        int id = 0;
        std::string name;
        bool isDir = false;
        int fileId = 0;   // 目录为 0
    };

    explicit FileNodeDao(Connection& conn) : m_conn(conn) {}

    // 按 (user, parent, name) 取子节点；不存在返回 false
    bool findChild(int userId, int parentId, const std::string& name, Row& out) const;

    // 按 (user, parent, name) 取**目录**子节点的 id；不存在返回 false
    bool findChildDirId(int userId, int parentId, const std::string& name, int& outId) const;

    // 插入子节点；fileIdOrZero <= 0 时写 NULL（目录）
    bool insert(int userId, int parentId, const std::string& name, bool isDir, int fileIdOrZero);

    // 列出某目录的直接子节点，按名字排序
    bool listChildren(int userId, int parentId, std::vector<Row>& out) const;

    bool deleteById(int nodeId);

    bool deleteByFileId(int fileId);

private:
    Connection& m_conn;
};
