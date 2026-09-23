#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <cstdint>
#include <thread>
#include <mutex>
#include <chrono>

#include "file_storage.pb.h"
#include "mprpc_application.h"
#include "mprpc_provider.h"
#include "zk_client_util.h"
#include "common/consistent_hash.h"
#include "database/CommonConnectionPool.hpp"

// 解析 "ip1:port1,ip2:port2" 形式的存储节点列表
static std::vector<StorageNode> parseStorageNodes(const std::string& str)
{
    std::vector<StorageNode> nodes;
    std::stringstream ss(str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        size_t colon = item.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        StorageNode node;
        node.ip = item.substr(0, colon);
        node.port = std::stoi(item.substr(colon + 1));
        nodes.push_back(node);
    }
    return nodes;
}

// 转义 SQL 字符串，防止注入
static std::string escapeSql(MYSQL* conn, const std::string& s)
{
    std::vector<char> buf(s.size() * 2 + 1, '\0');
    unsigned long len = mysql_real_escape_string(conn, buf.data(), s.c_str(), s.size());
    return std::string(buf.data(), len);
}

// 检测表是否存在指定列（查询失败时保守返回 true，跳过 ALTER）
static bool columnExists(Connection* conn, const std::string& table, const std::string& column)
{
    std::string sql =
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '" + table +
        "' AND COLUMN_NAME = '" + column + "'";
    MYSQL_RES* res = conn->query(sql);
    if (res == nullptr) {
        return true;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    bool exists = (row != nullptr && row[0] != nullptr && std::string(row[0]) != "0");
    mysql_free_result(res);
    return exists;
}

// 旧表缺列则 ALTER 补齐（幂等迁移）
static void addColumnIfMissing(Connection* conn, const std::string& table,
                               const std::string& column, const std::string& definition)
{
    if (!columnExists(conn, table, column)) {
        conn->update("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition);
    }
}

// 元数据服务：基于 MySQL 维护 文件名 -> 文件元数据（块位置）的索引
class MetaService : public filestore::MetaServiceRpc
{
public:
    MetaService()
    {
        // 初始化连接池（加载配置、预创建连接、启动生产/回收线程）
        ConnectionPool::getInstance();
        // 确保表存在
        createTablesIfNotExist();

        // 用静态配置作为初始种子（ZK 尚未发现节点时的兜底）
        std::string nodesStr = MprpcApplication::getConfig().load("storage_nodes");
        std::vector<StorageNode> seedNodes = parseStorageNodes(nodesStr);
        if (!seedNodes.empty()) {
            m_ring.build(seedNodes);
        }

        // 启动后台轮询线程：动态发现活跃存储节点（P11 动态扩缩容）
        std::thread([this]() { nodeWatchLoop(); }).detach();
    }

    // 上传登记：事务内查重 -> 插入 PENDING 状态索引，返回块分配方案
    void UploadFile(::google::protobuf::RpcController* controller,
                    const ::filestore::UploadFileRequest* request,
                    ::filestore::UploadFileResponse* response,
                    ::google::protobuf::Closure* done) override
    {
        bool noNode = false;
        {
            std::lock_guard<std::mutex> lock(m_ringMutex);
            noNode = m_ring.empty();
        }
        if (noNode) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("no active storage node");
            done->Run();
            return;
        }

        auto conn = ConnectionPool::getInstance().getConnection();
        if (!conn) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("get mysql connection failed");
            done->Run();
            return;
        }

        std::string filename = escapeSql(conn->getConn(), request->filename());

        conn->update("START TRANSACTION");

        // 查重：已存在（含 PENDING / COMPLETE）则拒绝
        MYSQL_RES* res = conn->query("SELECT filename FROM file_meta WHERE filename='" + filename + "'");
        if (res == nullptr) {
            conn->update("ROLLBACK");
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("query file_meta failed");
            done->Run();
            return;
        }
        bool exists = (mysql_num_rows(res) > 0);
        mysql_free_result(res);
        if (exists) {
            conn->update("ROLLBACK");
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("file already exists");
            done->Run();
            return;
        }

        // 计算块分配方案（一致性哈希：按 filename#chunk_index 落环）
        std::vector<filestore::ChunkLocation> chunks;
        {
            std::lock_guard<std::mutex> lock(m_ringMutex);
            for (int i = 0; i < request->chunk_count(); ++i) {
                StorageNode node = m_ring.locate(request->filename() + "#" + std::to_string(i));

                filestore::ChunkLocation loc;
                loc.set_chunk_index(i);
                loc.set_ip(node.ip);
                loc.set_port(node.port);
                chunks.push_back(loc);
            }
        }

        // 插入文件主记录（status=0 表示 PENDING）
        std::string insertMeta =
            "INSERT INTO file_meta(filename, filesize, chunk_count, status) VALUES('" +
            filename + "', " + std::to_string(request->filesize()) + ", " +
            std::to_string(request->chunk_count()) + ", 0)";
        if (!conn->update(insertMeta)) {
            conn->update("ROLLBACK");
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("insert file_meta failed");
            done->Run();
            return;
        }

        // 逐块插入块位置
        for (const auto& loc : chunks) {
            std::string insertChunk =
                "INSERT INTO file_chunk(filename, chunk_index, ip, port) VALUES('" +
                filename + "', " + std::to_string(loc.chunk_index()) + ", '" +
                escapeSql(conn->getConn(), loc.ip()) + "', " + std::to_string(loc.port()) + ")";
            if (!conn->update(insertChunk)) {
                conn->update("ROLLBACK");
                response->mutable_result()->set_errcode(1);
                response->mutable_result()->set_errmsg("insert file_chunk failed");
                done->Run();
                return;
            }
        }

        conn->update("COMMIT");

        // 填充响应：块分配方案
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        for (const auto& loc : chunks) {
            filestore::ChunkLocation* respLoc = response->add_chunks();
            respLoc->CopyFrom(loc);
        }

        std::cout << "register file: " << request->filename()
                  << " size:" << request->filesize()
                  << " chunks:" << request->chunk_count() << std::endl;

        done->Run();
    }

    // 上传完成确认：事务内 PENDING -> COMPLETE，并登记每块校验和
    void CommitUpload(::google::protobuf::RpcController* controller,
                      const ::filestore::CommitUploadRequest* request,
                      ::filestore::CommitUploadResponse* response,
                      ::google::protobuf::Closure* done) override
    {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (!conn) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("get mysql connection failed");
            done->Run();
            return;
        }

        std::string filename = escapeSql(conn->getConn(), request->filename());

        conn->update("START TRANSACTION");
        bool ok = conn->update(
            "UPDATE file_meta SET status=1 WHERE filename='" + filename + "' AND status=0");
        // 逐块登记校验和 + 紧凑存储偏移/大小
        for (int i = 0; i < request->chunks_size() && ok; ++i) {
            const auto& c = request->chunks(i);
            std::string checksum = escapeSql(conn->getConn(), c.checksum());
            ok = conn->update(
                "UPDATE file_chunk SET checksum='" + checksum +
                "', offset=" + std::to_string(c.offset()) +
                ", size=" + std::to_string(c.size()) +
                " WHERE filename='" + filename + "' AND chunk_index=" +
                std::to_string(c.chunk_index()));
        }

        if (ok) {
            conn->update("COMMIT");
            response->mutable_result()->set_errcode(0);
            response->mutable_result()->set_errmsg("");
        } else {
            conn->update("ROLLBACK");
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("commit upload failed");
        }
        done->Run();
    }

    // 上传失败回滚：删除 PENDING 状态索引（块清理由客户端补偿）
    void CancelUpload(::google::protobuf::RpcController* controller,
                      const ::filestore::CancelUploadRequest* request,
                      ::filestore::CancelUploadResponse* response,
                      ::google::protobuf::Closure* done) override
    {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (!conn) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("get mysql connection failed");
            done->Run();
            return;
        }

        std::string filename = escapeSql(conn->getConn(), request->filename());

        conn->update("START TRANSACTION");
        conn->update("DELETE FROM file_meta WHERE filename='" + filename + "' AND status=0");
        conn->update("DELETE FROM file_chunk WHERE filename='" + filename + "'");
        conn->update("COMMIT");

        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        done->Run();
    }

    // 查询文件块位置映射（只返回 COMPLETE 状态）
    void QueryFile(::google::protobuf::RpcController* controller,
                   const ::filestore::QueryFileRequest* request,
                   ::filestore::QueryFileResponse* response,
                   ::google::protobuf::Closure* done) override
    {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (!conn) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("get mysql connection failed");
            done->Run();
            return;
        }

        std::string filename = escapeSql(conn->getConn(), request->filename());

        MYSQL_RES* res = conn->query(
            "SELECT filesize, chunk_count FROM file_meta WHERE filename='" + filename + "' AND status=1");
        if (res == nullptr || mysql_num_rows(res) == 0) {
            if (res != nullptr) {
                mysql_free_result(res);
            }
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("file not found");
            done->Run();
            return;
        }

        MYSQL_ROW row = mysql_fetch_row(res);
        int64_t filesize = std::stoll(row[0]);
        int32_t chunkCount = std::stoi(row[1]);
        mysql_free_result(res);

        MYSQL_RES* chunkRes = conn->query(
            "SELECT chunk_index, ip, port, checksum, offset, size FROM file_chunk WHERE filename='" + filename + "' ORDER BY chunk_index");
        if (chunkRes == nullptr) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("query file_chunk failed");
            done->Run();
            return;
        }

        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        response->set_filesize(filesize);
        response->set_chunk_count(chunkCount);

        MYSQL_ROW crow;
        while ((crow = mysql_fetch_row(chunkRes)) != nullptr) {
            filestore::ChunkLocation* loc = response->add_chunks();
            loc->set_chunk_index(std::stoi(crow[0]));
            loc->set_ip(crow[1]);
            loc->set_port(std::stoi(crow[2]));
            loc->set_checksum(crow[3]);
            loc->set_offset(std::stoll(crow[4]));
            loc->set_size(std::stoi(crow[5]));
        }
        mysql_free_result(chunkRes);

        done->Run();
    }

    // 删除文件索引（块清理由客户端负责）
    void DeleteFile(::google::protobuf::RpcController* controller,
                    const ::filestore::DeleteFileRequest* request,
                    ::filestore::DeleteFileResponse* response,
                    ::google::protobuf::Closure* done) override
    {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (!conn) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("get mysql connection failed");
            done->Run();
            return;
        }

        std::string filename = escapeSql(conn->getConn(), request->filename());

        conn->update("START TRANSACTION");
        bool ok1 = conn->update("DELETE FROM file_meta WHERE filename='" + filename + "'");
        bool ok2 = conn->update("DELETE FROM file_chunk WHERE filename='" + filename + "'");
        conn->update("COMMIT");

        if (ok1 && ok2) {
            response->mutable_result()->set_errcode(0);
            response->mutable_result()->set_errmsg("");
            std::cout << "delete file index: " << request->filename() << std::endl;
        } else {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("delete file index failed");
        }
        done->Run();
    }

    // 查询某文件涉及的（去重后的）存储节点列表（P13 删除去重，替代客户端遍历 chunks 去重）
    void GetFileNodes(::google::protobuf::RpcController* controller,
                      const ::filestore::GetFileNodesRequest* request,
                      ::filestore::GetFileNodesResponse* response,
                      ::google::protobuf::Closure* done) override
    {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (!conn) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("get mysql connection failed");
            done->Run();
            return;
        }

        std::string filename = escapeSql(conn->getConn(), request->filename());

        MYSQL_RES* res = conn->query(
            "SELECT DISTINCT ip, port FROM file_chunk WHERE filename='" + filename + "'");
        if (res == nullptr) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("query file_chunk failed");
            done->Run();
            return;
        }

        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");

        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
            filestore::StorageNode* node = response->add_nodes();
            node->set_ip(row[0]);
            node->set_port(std::stoi(row[1]));
        }
        mysql_free_result(res);

        done->Run();
    }

private:
    // 建表（幂等）
    void createTablesIfNotExist()
    {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (!conn) {
            return;
        }
        conn->update(
            "CREATE TABLE IF NOT EXISTS file_meta ("
            "filename VARCHAR(255) PRIMARY KEY, "
            "filesize BIGINT NOT NULL, "
            "chunk_count INT NOT NULL, "
            "status TINYINT NOT NULL DEFAULT 0, "
            "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
            ") ENGINE=InnoDB");
        conn->update(
            "CREATE TABLE IF NOT EXISTS file_chunk ("
            "filename VARCHAR(255) NOT NULL, "
            "chunk_index INT NOT NULL, "
            "ip VARCHAR(64) NOT NULL, "
            "port INT NOT NULL, "
            "checksum VARCHAR(32) NOT NULL DEFAULT '', "
            "offset BIGINT NOT NULL DEFAULT 0, "
            "size INT NOT NULL DEFAULT 0, "
            "PRIMARY KEY (filename, chunk_index)"
            ") ENGINE=InnoDB");

        // 旧库迁移：CREATE TABLE IF NOT EXISTS 不会给已存在的旧表补新列，
        // 这里对 file_chunk 的 checksum/offset/size 列做幂等补齐（缺哪个加哪个）。
        addColumnIfMissing(conn.get(), "file_chunk", "checksum", "VARCHAR(32) NOT NULL DEFAULT ''");
        addColumnIfMissing(conn.get(), "file_chunk", "offset", "BIGINT NOT NULL DEFAULT 0");
        addColumnIfMissing(conn.get(), "file_chunk", "size", "INT NOT NULL DEFAULT 0");
    }

private:
    std::mutex m_ringMutex;    // 保护 m_ring（轮询线程写、UploadFile 读）
    ConsistentHash m_ring;     // 一致性哈希环，由 ZK 轮询线程动态重建

    // 后台线程：轮询 ZK 临时节点，动态维护活跃存储节点集合（P11 动态扩缩容）
    void nodeWatchLoop()
    {
        ZKClient zk;
        zk.start();   // 阻塞等待连接（最多 3s）

        std::string serviceName(filestore::StorageServiceRpc::descriptor()->name());
        std::string nodePath = "/" + serviceName + "/PutChunk";

        while (true) {
            std::vector<std::string> children = zk.getChildren(nodePath);
            if (children.empty()) {
                // 防御 ZK 抖动：空结果保留旧环，不轻易清空节点集合
                std::cerr << "[meta] no storage node from ZK, keep current ring" << std::endl;
            } else {
                std::vector<StorageNode> nodes;
                nodes.reserve(children.size());
                for (const std::string& host : children) {
                    size_t colon = host.find(':');
                    if (colon == std::string::npos) {
                        continue;
                    }
                    StorageNode node;
                    node.ip = host.substr(0, colon);
                    node.port = std::stoi(host.substr(colon + 1));
                    nodes.push_back(node);
                }
                if (!nodes.empty()) {
                    std::lock_guard<std::mutex> lock(m_ringMutex);
                    m_ring.build(nodes);
                    std::cerr << "[meta] refreshed storage nodes, count:" << nodes.size() << std::endl;
                }
            }
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
};

int main(int argc, char** argv)
{
    // 框架初始化
    MprpcApplication::init(argc, argv);

    // 注册元数据服务并启动
    RpcProvider provider;
    provider.notifyService(new MetaService());
    provider.run();

    return 0;
}
