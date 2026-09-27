#pragma once

#include "../Connection.hpp"

// 建表与迁移。
//
// 这段 DDL 原先写在 MetaService::createTablesIfNotExist() 里，和业务逻辑混在一个文件；
// 现独立出来，元数据服务启动时调用一次 Schema::ensure()。
class Schema
{
public:
    // 幂等：CREATE TABLE IF NOT EXISTS + 按需 ALTER。
    // 迁移一律"只扩不缩、不重写数据"——旧数据要么由登录路径顺手升级（口令散列），
    // 要么本来就兼容（新增索引）。
    static bool ensure(Connection& conn);
};
