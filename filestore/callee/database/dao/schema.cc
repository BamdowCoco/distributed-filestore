#include "schema.h"

#include "sql.h"

bool Schema::ensure(Connection& conn)
{
    // ---- 建表（幂等） ----
    if (!conn.update(
            "CREATE TABLE IF NOT EXISTS user ("
            "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '用户ID', "
            "username VARCHAR(64) NOT NULL COMMENT '用户名', "
            "password_hash VARCHAR(64) NOT NULL COMMENT '密码散列(SHA256)', "
            "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间', "
            "UNIQUE KEY uk_username (username)"
            ") ENGINE=InnoDB COMMENT='用户账号'")) {
        return false;
    }

    if (!conn.update(
            "CREATE TABLE IF NOT EXISTS file_meta ("
            "id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'file_id(数据文件以此命名)', "
            "filesize BIGINT NOT NULL COMMENT '文件大小（字节）', "
            "chunk_count INT NOT NULL COMMENT '分块数', "
            "status TINYINT NOT NULL DEFAULT 0 COMMENT '状态：0=PENDING 1=COMPLETE', "
            "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'"
            ") ENGINE=InnoDB COMMENT='文件数据元数据'")) {
        return false;
    }

    if (!conn.update(
            "CREATE TABLE IF NOT EXISTS file_node ("
            "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '节点ID', "
            "user_id INT NOT NULL COMMENT '所属用户', "
            "parent_id INT NOT NULL DEFAULT 0 COMMENT '父目录ID(0=根)', "
            "name VARCHAR(255) NOT NULL COMMENT '节点名(文件名或目录名)', "
            "is_dir TINYINT NOT NULL DEFAULT 0 COMMENT '0=文件 1=目录', "
            "file_id INT DEFAULT NULL COMMENT '文件对应的 file_meta.id(目录为 NULL)', "
            "UNIQUE KEY uk_parent_name (user_id, parent_id, name), "
            "KEY idx_user (user_id)"
            ") ENGINE=InnoDB COMMENT='虚拟文件树(同一目录下不重名)'")) {
        return false;
    }

    if (!conn.update(
            "CREATE TABLE IF NOT EXISTS file_chunk ("
            "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键', "
            "file_id INT NOT NULL COMMENT '所属文件唯一标识', "
            "chunk_index INT NOT NULL COMMENT '块序号', "
            "ip VARCHAR(64) NOT NULL COMMENT '存储节点 IP', "
            "port INT NOT NULL COMMENT '存储节点端口', "
            "checksum VARCHAR(32) NOT NULL DEFAULT '' COMMENT '块 MD5 校验和', "
            "offset BIGINT NOT NULL DEFAULT 0 COMMENT '块在数据文件内的紧凑存储偏移', "
            "size INT NOT NULL DEFAULT 0 COMMENT '块实际大小（字节）', "
            "UNIQUE KEY uk_file_chunk (file_id, chunk_index), "
            "KEY idx_file_id (file_id)"
            ") ENGINE=InnoDB COMMENT='文件块位置索引'")) {
        return false;
    }

    if (!conn.update(
            "CREATE TABLE IF NOT EXISTS cleanup_queue ("
            "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '任务 ID', "
            "node_ip VARCHAR(64) NOT NULL COMMENT '存储节点 IP', "
            "node_port INT NOT NULL COMMENT '存储节点端口', "
            "file_id INT NOT NULL COMMENT '待清理的数据文件唯一标识', "
            "status TINYINT NOT NULL DEFAULT 0 COMMENT '状态：0=待清理 1=已完成 3=需人工', "
            "retry_count INT NOT NULL DEFAULT 0 COMMENT '已重试次数', "
            "next_retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '下次重试时间（指数退避）', "
            "UNIQUE KEY uk_task (node_ip, node_port, file_id), "
            "KEY idx_status_retry (status, next_retry_at)"
            ") ENGINE=InnoDB COMMENT='待清理任务队列（删块失败的孤儿块）'")) {
        return false;
    }

    // ---- 迁移 1：file_node.file_id 索引 ----
    // CREATE TABLE IF NOT EXISTS 对已存在的表不生效，而按 file_id 删节点
    //（删除文件 / 取消上传）会因缺索引退化为全表扫描。
    {
        MYSQL_RES* res = conn.query(
            "SELECT COUNT(*) FROM information_schema.statistics "
            "WHERE table_schema = DATABASE() AND table_name = 'file_node' "
            "AND index_name = 'idx_file_id'");
        if (res == nullptr) {
            return false;
        }
        int idxCount = 0;
        MYSQL_ROW row = mysql_fetch_row(res);
        colInt(row, 0, idxCount);
        mysql_free_result(res);
        if (idxCount == 0) {
            conn.update("ALTER TABLE file_node ADD KEY idx_file_id (file_id)");
        }
    }

    // ---- 迁移 2：password_hash 扩宽 ----
    // 带盐散列格式 pbkdf2-sha256$<iter>$<b64盐>$<b64散列> 约 90 字符，早期是 VARCHAR(64)。
    // 只扩不缩、不重写数据：旧值在下次登录验证通过时由 Login 顺手升级。
    {
        MYSQL_RES* res = conn.query(
            "SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = 'user' "
            "AND column_name = 'password_hash'");
        if (res == nullptr) {
            return false;
        }
        int colLen = 0;
        MYSQL_ROW row = mysql_fetch_row(res);
        colInt(row, 0, colLen);
        mysql_free_result(res);
        if (colLen > 0 && colLen < 160) {
            conn.update(
                "ALTER TABLE user MODIFY password_hash VARCHAR(160) NOT NULL "
                "COMMENT '密码散列(PBKDF2-SHA256 带盐; 兼容历史无盐 SHA256)'");
        }
    }

    // ---- 迁移 3：cleanup_queue.file_id 索引 ----
    // 分区间对账会按 file_id 区间查队列在管的集合；没有这个索引就是全表扫描。
    {
        MYSQL_RES* res = conn.query(
            "SELECT COUNT(*) FROM information_schema.statistics "
            "WHERE table_schema = DATABASE() AND table_name = 'cleanup_queue' "
            "AND index_name = 'idx_file_id'");
        if (res == nullptr) {
            return false;
        }
        int idxCount = 0;
        MYSQL_ROW row = mysql_fetch_row(res);
        colInt(row, 0, idxCount);
        mysql_free_result(res);
        if (idxCount == 0) {
            conn.update("ALTER TABLE cleanup_queue ADD KEY idx_file_id (file_id)");
        }
    }

    return true;
}
