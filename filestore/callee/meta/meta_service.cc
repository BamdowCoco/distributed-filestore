#include "meta_service.h"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <set>
#include <sstream>
#include <thread>

#include "database/CommonConnectionPool.hpp"
#include "mprpc_application.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "zk_client_util.h"

// P20 待清理队列：Redis Stream 名 / 消费者组 / 消费者名
constexpr const char* kCleanupStream = "cleanup_queue";
constexpr const char* kCleanupGroup = "cleanup-group";
constexpr const char* kCleanupConsumer = "meta-cleanup";
// P20 分片扫描参数（60s/轮 → 24h 覆盖全量；验证可临时改小）
constexpr int kShardCount = 1440;
// P20 待清理任务最大重试次数，达到后进入终态 status=3（需人工）
constexpr int kMaxRetry = 10;

// 鉴权：Redis session key 前缀与 token 有效期（秒）
constexpr const char* kSessionPrefix = "session:";
constexpr int kTokenTtl = 86400;   // 1 天

namespace {

// 解析 "ip1:port1,ip2:port2" 形式的存储节点列表
std::vector<StorageNode> parseStorageNodes(const std::string& str)
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
std::string escapeSql(MYSQL* conn, const std::string& s)
{
    std::vector<char> buf(s.size() * 2 + 1, '\0');
    unsigned long len = mysql_real_escape_string(conn, buf.data(), s.c_str(), s.size());
    return std::string(buf.data(), len);
}

// 按 '/' 切分虚拟路径（忽略空段，如 "/a/b" -> ["a","b"]）
std::vector<std::string> splitPath(const std::string& path)
{
    std::vector<std::string> parts;
    std::stringstream ss(path);
    std::string item;
    while (std::getline(ss, item, '/')) {
        if (!item.empty()) {
            parts.push_back(item);
        }
    }
    return parts;
}

// 校验虚拟路径：深度≤64、总路径≤1024 字符、单级≤255 字节，合法返回 true
bool validatePath(const std::string& path)
{
    if (path.size() > 1024) {
        return false;
    }
    std::vector<std::string> parts = splitPath(path);
    if (parts.size() > 64) {
        return false;
    }
    for (const auto& p : parts) {
        if (p.size() > 255) {
            return false;
        }
    }
    return true;
}

// 生成随机 token（32 字节十六进制）
std::string generateToken()
{
    static const char* hex = "0123456789abcdef";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);
    std::string token;
    for (int i = 0; i < 32; ++i) {
        token.push_back(hex[dis(gen) >> 4]);
        token.push_back(hex[dis(gen) & 0x0F]);
    }
    return token;
}

}  // namespace

// 构造：初始化连接池/建表/Redis，启动后台线程（节点发现 + 待清理队列消费/退避 + 分片扫描）
MetaService::MetaService()
{
    ConnectionPool::getInstance();
    createTablesIfNotExist();

    m_redis.connect();
    m_redis.xgroupCreate(kCleanupStream, kCleanupGroup);   // 幂等创建消费者组

    std::string nodesStr = MprpcApplication::getConfig().load("storage_nodes");
    std::vector<StorageNode> seedNodes = parseStorageNodes(nodesStr);
    if (!seedNodes.empty()) {
        m_ring.build(seedNodes);
        m_nodes = seedNodes;
    }

    std::thread([this]() { nodeWatchLoop(); }).detach();
    std::thread([this]() { cleanupQueueLoop(); }).detach();
    std::thread([this]() { cleanupRetryLoop(); }).detach();
    std::thread([this]() { shardScanLoop(); }).detach();
}

// ===================== 鉴权 =====================

// 注册：插入 user 表（用户名唯一，密码 SHA256 散列）
void MetaService::Register(::google::protobuf::RpcController* controller,
                           const ::filestore::RegisterRequest* request,
                           ::filestore::RegisterResponse* response,
                           ::google::protobuf::Closure* done)
{
    auto conn = ConnectionPool::getInstance().getConnection();
    if (!conn) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("get mysql connection failed");
        done->Run();
        return;
    }
    std::string username = escapeSql(conn->getConn(), request->username());
    std::string hash = sha256Hex(request->password());

    // 查重：用户名已存在则拒绝
    MYSQL_RES* res = conn->query("SELECT id FROM user WHERE username='" + username + "'");
    if (res != nullptr && mysql_num_rows(res) > 0) {
        mysql_free_result(res);
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("username already exists");
        done->Run();
        return;
    }
    if (res != nullptr) {
        mysql_free_result(res);
    }

    // 插入新用户
    if (conn->update("INSERT INTO user(username, password_hash) VALUES('" + username + "', '" + hash + "')")) {
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
    } else {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("register failed");
    }
    done->Run();
}

// 登录：校验账号密码，签发随机 token 存 Redis（TTL 1 天）
void MetaService::Login(::google::protobuf::RpcController* controller,
                        const ::filestore::LoginRequest* request,
                        ::filestore::LoginResponse* response,
                        ::google::protobuf::Closure* done)
{
    auto conn = ConnectionPool::getInstance().getConnection();
    if (!conn) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("get mysql connection failed");
        done->Run();
        return;
    }
    std::string username = escapeSql(conn->getConn(), request->username());
    std::string hash = sha256Hex(request->password());

    // 校验账号密码
    MYSQL_RES* res = conn->query(
        "SELECT id FROM user WHERE username='" + username + "' AND password_hash='" + hash + "'");
    if (res == nullptr || mysql_num_rows(res) == 0) {
        if (res != nullptr) {
            mysql_free_result(res);
        }
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid username or password");
        done->Run();
        return;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    int userId = std::stoi(row[0]);
    mysql_free_result(res);

    // 签发随机 token 存 Redis（TTL 1 天）
    std::string token = generateToken();
    m_redis.set(kSessionPrefix + token, std::to_string(userId), kTokenTtl);

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_token(token);
    response->set_user_id(userId);
    done->Run();
}

// 登出：删除 Redis 中的会话 token
void MetaService::Logout(::google::protobuf::RpcController* controller,
                         const ::filestore::LogoutRequest* request,
                         ::filestore::LogoutResponse* response,
                         ::google::protobuf::Closure* done)
{
    m_redis.del(kSessionPrefix + request->token());
    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    done->Run();
}

// ===================== 目录 =====================

// 建目录：解析路径，父目录须已存在、同目录不重名
void MetaService::Mkdir(::google::protobuf::RpcController* controller,
                        const ::filestore::MkdirRequest* request,
                        ::filestore::MkdirResponse* response,
                        ::google::protobuf::Closure* done)
{
    int userId = authenticate(request->token());
    if (userId == 0) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("not logged in");
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

    // 校验路径合法性（深度 ≤64 / 总长 ≤1024 / 单级 ≤255）
    if (!validatePath(request->path())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid path");
        done->Run();
        return;
    }

    std::vector<std::string> parts = splitPath(request->path());
    if (parts.empty()) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid path");
        done->Run();
        return;
    }

    // 解析父目录（路径去掉最后一段）
    int parentId = resolveParentDir(*conn, userId, parts);
    if (parentId < 0) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("parent dir not found");
        done->Run();
        return;
    }
    // 同目录去重
    if (nodeExists(*conn, userId, parentId, parts.back())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("already exists");
        done->Run();
        return;
    }

    // 插入目录节点
    std::string name = escapeSql(conn->getConn(), parts.back());
    if (conn->update(
            "INSERT INTO file_node(user_id, parent_id, name, is_dir, file_id) VALUES(" +
            std::to_string(userId) + ", " + std::to_string(parentId) + ", '" + name + "', 1, NULL)")) {
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
    } else {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("mkdir failed");
    }
    done->Run();
}

// 删目录：非递归只删空目录；recursive 递归删除其下所有子目录与文件
void MetaService::Rmdir(::google::protobuf::RpcController* controller,
                        const ::filestore::RmdirRequest* request,
                        ::filestore::RmdirResponse* response,
                        ::google::protobuf::Closure* done)
{
    int userId = authenticate(request->token());
    if (userId == 0) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("not logged in");
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

    if (!validatePath(request->path())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid path");
        done->Run();
        return;
    }

    // 解析目录节点 id：根目录 "/" 没有对应 file_node 行，用 id=0 表示
    int dirId = 0;
    if (request->path() != "/") {
        PathNode node = resolvePath(*conn, userId, request->path());
        if (node.id == 0 || !node.isDir) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("dir not found");
            done->Run();
            return;
        }
        dirId = node.id;
    }

    if (request->recursive()) {
        // 递归删除：删所有后代元数据 + 块数据入待清理队列
        removeDirRecursive(*conn, userId, dirId);
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        done->Run();
        return;
    }

    // 非递归：只删空目录
    MYSQL_RES* res = conn->query(
        "SELECT id FROM file_node WHERE user_id=" + std::to_string(userId) +
        " AND parent_id=" + std::to_string(dirId));
    bool hasChild = (res != nullptr && mysql_num_rows(res) > 0);
    if (res != nullptr) {
        mysql_free_result(res);
    }
    if (hasChild) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("dir not empty");
        done->Run();
        return;
    }

    // 删除目录节点（根目录 id=0 无行可删，直接返回成功）
    if (dirId == 0 || conn->update("DELETE FROM file_node WHERE id=" + std::to_string(dirId))) {
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
    } else {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("rmdir failed");
    }
    done->Run();
}

// 列目录：返回指定目录下的子节点（名字 / 类型 / file_id）
void MetaService::ListDir(::google::protobuf::RpcController* controller,
                          const ::filestore::ListDirRequest* request,
                          ::filestore::ListDirResponse* response,
                          ::google::protobuf::Closure* done)
{
    int userId = authenticate(request->token());
    if (userId == 0) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("not logged in");
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

    // 根目录(/)直接查，其余先解析出目录节点 id
    int dirId = 0;
    std::string path = request->path();
    if (path != "/") {
        PathNode node = resolvePath(*conn, userId, path);
        if (node.id == 0 || !node.isDir) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("dir not found");
            done->Run();
            return;
        }
        dirId = node.id;
    }

    // 查询该目录下的子节点
    MYSQL_RES* res = conn->query(
        "SELECT name, is_dir, COALESCE(file_id,0) FROM file_node WHERE user_id=" +
        std::to_string(userId) + " AND parent_id=" + std::to_string(dirId) + " ORDER BY name");
    if (res == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("list dir failed");
        done->Run();
        return;
    }

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        filestore::DirEntry* e = response->add_entries();
        e->set_name(row[0]);
        e->set_is_dir(std::stoi(row[1]) != 0);
        e->set_file_id(std::stoi(row[2]));
    }
    mysql_free_result(res);
    done->Run();
}

// ===================== 文件 =====================

// 上传登记：查重后分配 file_id，插入 file_node/file_chunk，返回块分配方案
void MetaService::UploadFile(::google::protobuf::RpcController* controller,
                             const ::filestore::UploadFileRequest* request,
                             ::filestore::UploadFileResponse* response,
                             ::google::protobuf::Closure* done)
{
    int userId = authenticate(request->token());
    if (userId == 0) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("not logged in");
        done->Run();
        return;
    }

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

    // 校验路径合法性（深度 ≤64 / 总长 ≤1024 / 单级 ≤255）
    if (!validatePath(request->path())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid path");
        done->Run();
        return;
    }

    std::vector<std::string> parts = splitPath(request->path());
    if (parts.empty()) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid path");
        done->Run();
        return;
    }

    // 解析父目录（路径去掉最后一段）
    int parentId = resolveParentDir(*conn, userId, parts);
    if (parentId < 0) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("parent dir not found");
        done->Run();
        return;
    }
    // 同目录去重
    if (nodeExists(*conn, userId, parentId, parts.back())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("file already exists");
        done->Run();
        return;
    }

    // 事务：插入 file_meta 分配 file_id
    conn->update("START TRANSACTION");
    if (!conn->update(
            "INSERT INTO file_meta(filesize, chunk_count, status) VALUES(" +
            std::to_string(request->filesize()) + ", " +
            std::to_string(request->chunk_count()) + ", 0)")) {
        conn->update("ROLLBACK");
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("insert file_meta failed");
        done->Run();
        return;
    }

    // 取刚分配的自增 file_id
    int32_t fileId = 0;
    MYSQL_RES* idRes = conn->query("SELECT LAST_INSERT_ID()");
    if (idRes != nullptr) {
        MYSQL_ROW idRow = mysql_fetch_row(idRes);
        if (idRow != nullptr && idRow[0] != nullptr) {
            fileId = static_cast<int32_t>(std::stoi(idRow[0]));
        }
        mysql_free_result(idRes);
    }
    if (fileId <= 0) {
        conn->update("ROLLBACK");
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("get file_id failed");
        done->Run();
        return;
    }

    // 插入文件节点（叶子节点，关联 file_id）
    std::string nameEsc = escapeSql(conn->getConn(), parts.back());
    if (!conn->update(
            "INSERT INTO file_node(user_id, parent_id, name, is_dir, file_id) VALUES(" +
            std::to_string(userId) + ", " + std::to_string(parentId) + ", '" + nameEsc + "', 0, " +
            std::to_string(fileId) + ")")) {
        conn->update("ROLLBACK");
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("insert file_node failed");
        done->Run();
        return;
    }
    conn->update("COMMIT");

    // 一致性哈希分配块（按 file_id#chunk_index 落环）
    std::vector<filestore::ChunkLocation> chunks;
    {
        std::lock_guard<std::mutex> lock(m_ringMutex);
        for (int i = 0; i < request->chunk_count(); ++i) {
            StorageNode node = m_ring.locate(std::to_string(fileId) + "#" + std::to_string(i));
            filestore::ChunkLocation loc;
            loc.set_chunk_index(i);
            loc.set_ip(node.ip);
            loc.set_port(node.port);
            chunks.push_back(loc);
        }
    }

    // 逐块插入块位置
    for (const auto& loc : chunks) {
        std::string insertChunk =
            "INSERT INTO file_chunk(file_id, chunk_index, ip, port) VALUES(" +
            std::to_string(fileId) + ", " + std::to_string(loc.chunk_index()) + ", '" +
            escapeSql(conn->getConn(), loc.ip()) + "', " + std::to_string(loc.port()) + ")";
        if (!conn->update(insertChunk)) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("insert file_chunk failed");
            done->Run();
            return;
        }
    }

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_file_id(fileId);
    for (const auto& loc : chunks) {
        filestore::ChunkLocation* respLoc = response->add_chunks();
        respLoc->CopyFrom(loc);
    }

    std::cout << "register file: " << request->path()
              << " id:" << fileId
              << " size:" << request->filesize()
              << " chunks:" << request->chunk_count() << std::endl;
    done->Run();
}

// 提交上传：PENDING->COMPLETE，登记每块 checksum/offset/size
void MetaService::CommitUpload(::google::protobuf::RpcController* controller,
                               const ::filestore::CommitUploadRequest* request,
                               ::filestore::CommitUploadResponse* response,
                               ::google::protobuf::Closure* done)
{
    auto conn = ConnectionPool::getInstance().getConnection();
    if (!conn) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("get mysql connection failed");
        done->Run();
        return;
    }
    int32_t fileId = request->file_id();

    // 事务：PENDING -> COMPLETE，逐块登记校验和/偏移/大小
    conn->update("START TRANSACTION");
    bool ok = conn->update(
        "UPDATE file_meta SET status=1 WHERE id=" + std::to_string(fileId) + " AND status=0");
    for (int i = 0; i < request->chunks_size() && ok; ++i) {
        const auto& c = request->chunks(i);
        std::string checksum = escapeSql(conn->getConn(), c.checksum());
        ok = conn->update(
            "UPDATE file_chunk SET checksum='" + checksum +
            "', offset=" + std::to_string(c.offset()) +
            ", size=" + std::to_string(c.size()) +
            " WHERE file_id=" + std::to_string(fileId) + " AND chunk_index=" +
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

// 取消上传：删除 PENDING 状态的元数据记录（上传失败回滚）
void MetaService::CancelUpload(::google::protobuf::RpcController* controller,
                               const ::filestore::CancelUploadRequest* request,
                               ::filestore::CancelUploadResponse* response,
                               ::google::protobuf::Closure* done)
{
    auto conn = ConnectionPool::getInstance().getConnection();
    if (!conn) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("get mysql connection failed");
        done->Run();
        return;
    }
    int32_t fileId = request->file_id();

    // 事务：删除 PENDING 状态的元数据（三表）
    conn->update("START TRANSACTION");
    conn->update("DELETE FROM file_meta WHERE id=" + std::to_string(fileId) + " AND status=0");
    conn->update("DELETE FROM file_chunk WHERE file_id=" + std::to_string(fileId));
    conn->update("DELETE FROM file_node WHERE file_id=" + std::to_string(fileId));
    conn->update("COMMIT");

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    done->Run();
}

// 查询文件：路径->file_id，返回块位置映射（仅 COMPLETE 状态）
void MetaService::QueryFile(::google::protobuf::RpcController* controller,
                            const ::filestore::QueryFileRequest* request,
                            ::filestore::QueryFileResponse* response,
                            ::google::protobuf::Closure* done)
{
    int userId = authenticate(request->token());
    if (userId == 0) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("not logged in");
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

    // 路径 -> file_id
    PathNode node = resolvePath(*conn, userId, request->path());
    if (node.id == 0 || node.isDir) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("file not found");
        done->Run();
        return;
    }
    int32_t fileId = node.fileId;

    // 查文件元数据（仅 COMPLETE）
    MYSQL_RES* res = conn->query(
        "SELECT filesize, chunk_count FROM file_meta WHERE id=" + std::to_string(fileId) + " AND status=1");
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

    // 查块位置映射（按 chunk_index 排序回填）
    MYSQL_RES* chunkRes = conn->query(
        "SELECT chunk_index, ip, port, checksum, offset, size FROM file_chunk WHERE file_id=" +
        std::to_string(fileId) + " ORDER BY chunk_index");
    if (chunkRes == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("query file_chunk failed");
        done->Run();
        return;
    }

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_file_id(fileId);
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

// 删除文件索引：删 file_node/file_meta/file_chunk（块数据由客户端删）
void MetaService::DeleteFile(::google::protobuf::RpcController* controller,
                             const ::filestore::DeleteFileRequest* request,
                             ::filestore::DeleteFileResponse* response,
                             ::google::protobuf::Closure* done)
{
    int userId = authenticate(request->token());
    if (userId == 0) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("not logged in");
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
    int32_t fileId = request->file_id();

    // 事务：删 file_meta / file_chunk / file_node
    conn->update("START TRANSACTION");
    bool ok1 = conn->update("DELETE FROM file_meta WHERE id=" + std::to_string(fileId));
    bool ok2 = conn->update("DELETE FROM file_chunk WHERE file_id=" + std::to_string(fileId));
    conn->update("DELETE FROM file_node WHERE file_id=" + std::to_string(fileId));
    conn->update("COMMIT");

    if (ok1 && ok2) {
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        std::cout << "delete file index id:" << fileId << std::endl;
    } else {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("delete file index failed");
    }
    done->Run();
}

// 查询文件涉及的存储节点（去重后的 ip:port 列表，用于删除）
void MetaService::GetFileNodes(::google::protobuf::RpcController* controller,
                               const ::filestore::GetFileNodesRequest* request,
                               ::filestore::GetFileNodesResponse* response,
                               ::google::protobuf::Closure* done)
{
    int userId = authenticate(request->token());
    if (userId == 0) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("not logged in");
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

    // 路径 -> file_id
    PathNode node = resolvePath(*conn, userId, request->path());
    if (node.id == 0 || node.isDir) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("file not found");
        done->Run();
        return;
    }
    int32_t fileId = node.fileId;

    // 查该文件涉及的（去重后的）存储节点
    MYSQL_RES* res = conn->query(
        "SELECT DISTINCT ip, port FROM file_chunk WHERE file_id=" + std::to_string(fileId));
    if (res == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("query file_chunk failed");
        done->Run();
        return;
    }

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_file_id(fileId);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        filestore::StorageNode* n = response->add_nodes();
        n->set_ip(row[0]);
        n->set_port(std::stoi(row[1]));
    }
    mysql_free_result(res);
    done->Run();
}

// 待清理任务入队：删块失败后由客户端上报，MySQL 持久化 + Redis Stream 推任务 ID
void MetaService::AddCleanupTask(::google::protobuf::RpcController* controller,
                                 const ::filestore::AddCleanupTaskRequest* request,
                                 ::filestore::AddCleanupTaskResponse* response,
                                 ::google::protobuf::Closure* done)
{
    enqueueCleanup(request->node_ip(), request->node_port(), request->file_id());
    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    done->Run();
}

// ===================== 私有辅助 =====================

// 鉴权：token -> user_id，失败（未登录/过期）返回 0
int MetaService::authenticate(const std::string& token)
{
    if (token.empty()) {
        return 0;
    }
    std::string val = m_redis.get(kSessionPrefix + token);
    return val.empty() ? 0 : std::stoi(val);
}

// 按路径沿 parent_id 逐级查找节点，返回末尾节点的 id/file_id/isDir（不存在 id=0）
MetaService::PathNode MetaService::resolvePath(Connection& conn, int userId, const std::string& path)
{
    PathNode result;
    std::vector<std::string> parts = splitPath(path);
    int parentId = 0;
    for (size_t i = 0; i < parts.size(); ++i) {
        std::string name = escapeSql(conn.getConn(), parts[i]);
        MYSQL_RES* res = conn.query(
            "SELECT id, is_dir, COALESCE(file_id,0) FROM file_node WHERE user_id=" +
            std::to_string(userId) + " AND parent_id=" + std::to_string(parentId) +
            " AND name='" + name + "'");
        if (res == nullptr || mysql_num_rows(res) == 0) {
            if (res != nullptr) {
                mysql_free_result(res);
            }
            result.id = 0;
            return result;
        }
        MYSQL_ROW row = mysql_fetch_row(res);
        result.id = std::stoi(row[0]);
        result.isDir = (std::stoi(row[1]) != 0);
        result.fileId = std::stoi(row[2]);
        mysql_free_result(res);
        parentId = result.id;
    }
    return result;
}

// 解析父目录：给定已切分的路径 parts（如 ["a","b","c.txt"]），
// 逐级查找除最后一段外的每一级目录（"a"、"b"），返回最后一级目录 "b" 的节点 id；
// 中途任一级目录不存在（或不是目录）则返回 -1。
int MetaService::resolveParentDir(Connection& conn, int userId, const std::vector<std::string>& parts)
{
    int parentId = 0;   // 从根目录（parent_id=0）开始逐级向下
    // 只遍历除最后一段外的所有段（i+1 < parts.size() 保证跳过最后一段，如跳过 "c.txt"）
    for (size_t i = 0; i + 1 < parts.size(); ++i) {
        std::string name = escapeSql(conn.getConn(), parts[i]);

        // 在当前 parentId 目录下，查找名为 name 的【目录】节点（is_dir=1）
        MYSQL_RES* res = conn.query(
            "SELECT id FROM file_node WHERE user_id=" + std::to_string(userId) +
            " AND parent_id=" + std::to_string(parentId) + " AND name='" + name + "' AND is_dir=1");
        if (res == nullptr || mysql_num_rows(res) == 0) {
            // 这一级目录不存在 → 父目录路径无效
            if (res != nullptr) {
                mysql_free_result(res);
            }
            return -1;
        }
        // 找到后，把 parentId 推进到这一级目录，作为下一级查找的起点
        MYSQL_ROW row = mysql_fetch_row(res);
        parentId = std::stoi(row[0]);
        mysql_free_result(res);
    }
    // 循环结束，parentId 即最后一级目录（父目录）的节点 id
    return parentId;
}

// 某名字在某父目录下是否已存在（同目录去重判断）
bool MetaService::nodeExists(Connection& conn, int userId, int parentId, const std::string& name)
{
    std::string nameEsc = escapeSql(conn.getConn(), name);
    MYSQL_RES* res = conn.query(
        "SELECT id FROM file_node WHERE user_id=" + std::to_string(userId) +
        " AND parent_id=" + std::to_string(parentId) + " AND name='" + nameEsc + "'");
    bool exists = (res != nullptr && mysql_num_rows(res) > 0);
    if (res != nullptr) {
        mysql_free_result(res);
    }
    return exists;
}

// 递归删除目录：BFS 收集后代 -> 删元数据 -> 块数据入待清理队列异步清理
void MetaService::removeDirRecursive(Connection& conn, int userId, int rootDirId)
{
    struct CleanupItem
    {
        std::string ip;
        int port;
        int fileId;
    };

    // BFS 沿 parent_id 收集目录下全部后代（目录 id + 文件 file_id）
    std::vector<int> dirIds{rootDirId};
    std::vector<int> nodeIds;   // 后代 file_node id（含目录与文件）
    std::vector<int> fileIds;   // 后代文件 file_id

    for (size_t idx = 0; idx < dirIds.size(); ++idx) {
        int dirId = dirIds[idx];
        MYSQL_RES* res = conn.query(
            "SELECT id, is_dir, COALESCE(file_id,0) FROM file_node WHERE user_id=" +
            std::to_string(userId) + " AND parent_id=" + std::to_string(dirId));
        if (res == nullptr) {
            continue;
        }
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
            int id = std::stoi(row[0]);
            bool isDir = std::stoi(row[1]) != 0;
            nodeIds.push_back(id);
            if (isDir) {
                dirIds.push_back(id);
            } else {
                fileIds.push_back(std::stoi(row[2]));
            }
        }
        mysql_free_result(res);
    }

    // 收集块数据清理任务（每个文件在每个存储节点上的数据）
    std::vector<CleanupItem> cleanupItems;
    for (int fileId : fileIds) {
        MYSQL_RES* res = conn.query(
            "SELECT DISTINCT ip, port FROM file_chunk WHERE file_id=" + std::to_string(fileId));
        if (res == nullptr) {
            continue;
        }
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
            cleanupItems.push_back({row[0], std::stoi(row[1]), fileId});
        }
        mysql_free_result(res);
    }

    // 删元数据：后代 file_node + 根目录 + file_meta + file_chunk
    for (int nodeId : nodeIds) {
        conn.update("DELETE FROM file_node WHERE id=" + std::to_string(nodeId));
    }
    conn.update("DELETE FROM file_node WHERE id=" + std::to_string(rootDirId));
    for (int fileId : fileIds) {
        conn.update("DELETE FROM file_meta WHERE id=" + std::to_string(fileId));
        conn.update("DELETE FROM file_chunk WHERE file_id=" + std::to_string(fileId));
    }

    // 块数据入待清理队列（后台清理线程异步删）
    for (const auto& item : cleanupItems) {
        enqueueCleanup(item.ip, item.port, item.fileId);
    }
}

// 建表（幂等）：user / file_meta / file_node / file_chunk / cleanup_queue
void MetaService::createTablesIfNotExist()
{
    auto conn = ConnectionPool::getInstance().getConnection();
    if (!conn) {
        return;
    }
    conn->update(
        "CREATE TABLE IF NOT EXISTS user ("
        "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '用户ID', "
        "username VARCHAR(64) NOT NULL COMMENT '用户名', "
        "password_hash VARCHAR(64) NOT NULL COMMENT '密码散列(SHA256)', "
        "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间', "
        "UNIQUE KEY uk_username (username)"
        ") ENGINE=InnoDB COMMENT='用户账号'");
    conn->update(
        "CREATE TABLE IF NOT EXISTS file_meta ("
        "id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'file_id(数据文件以此命名)', "
        "filesize BIGINT NOT NULL COMMENT '文件大小（字节）', "
        "chunk_count INT NOT NULL COMMENT '分块数', "
        "status TINYINT NOT NULL DEFAULT 0 COMMENT '状态：0=PENDING 1=COMPLETE', "
        "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'"
        ") ENGINE=InnoDB COMMENT='文件数据元数据'");
    conn->update(
        "CREATE TABLE IF NOT EXISTS file_node ("
        "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '节点ID', "
        "user_id INT NOT NULL COMMENT '所属用户', "
        "parent_id INT NOT NULL DEFAULT 0 COMMENT '父目录ID(0=根)', "
        "name VARCHAR(255) NOT NULL COMMENT '节点名(文件名或目录名)', "
        "is_dir TINYINT NOT NULL DEFAULT 0 COMMENT '0=文件 1=目录', "
        "file_id INT DEFAULT NULL COMMENT '文件对应的 file_meta.id(目录为 NULL)', "
        "UNIQUE KEY uk_parent_name (user_id, parent_id, name), "
        "KEY idx_user (user_id)"
        ") ENGINE=InnoDB COMMENT='虚拟文件树(同一目录下不重名)'");
    conn->update(
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
        ") ENGINE=InnoDB COMMENT='文件块位置索引'");
    conn->update(
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
        ") ENGINE=InnoDB COMMENT='待清理任务队列（删块失败的孤儿块）'");
}

// 后台线程：轮询 ZK 临时节点，动态维护活跃存储节点集合（P11 动态扩缩容）
void MetaService::nodeWatchLoop()
{
    ZKClient zk;
    zk.start();

    std::string serviceName(filestore::StorageServiceRpc::descriptor()->name());
    std::string nodePath = "/" + serviceName + "/PutChunk";

    while (true) {
        std::vector<std::string> children = zk.getChildren(nodePath);
        if (children.empty()) {
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
                m_nodes = nodes;
                std::cerr << "[meta] refreshed storage nodes, count:" << nodes.size() << std::endl;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }
}

// 待清理任务入队：MySQL 幂等插入 + Redis Stream 推任务 ID
void MetaService::enqueueCleanup(const std::string& ip, int port, int fileId)
{
    auto conn = ConnectionPool::getInstance().getConnection();
    if (!conn) {
        return;
    }
    std::string ipEsc = escapeSql(conn->getConn(), ip);

    // MySQL 幂等插入（同任务已存在则重置为待清理）
    conn->update(
        "INSERT INTO cleanup_queue(node_ip, node_port, file_id, status, retry_count, next_retry_at) "
        "VALUES('" + ipEsc + "', " + std::to_string(port) + ", " + std::to_string(fileId) + ", 0, 0, NOW()) "
        "ON DUPLICATE KEY UPDATE status=0, retry_count=0, next_retry_at=NOW()");

    // 取任务 ID 推入 Redis Stream（消费线程据此处理）
    MYSQL_RES* res = conn->query(
        "SELECT id FROM cleanup_queue WHERE node_ip='" + ipEsc + "' AND node_port=" +
        std::to_string(port) + " AND file_id=" + std::to_string(fileId));
    if (res != nullptr) {
        MYSQL_ROW row = mysql_fetch_row(res);
        if (row != nullptr && row[0] != nullptr) {
            m_redis.xadd(kCleanupStream, "id", row[0]);
        }
        mysql_free_result(res);
    }
}

// 处理单个待清理任务：查信息 -> 检查退避到期 -> 直连删数据 -> 成功标完成 / 失败退避或终态
void MetaService::processCleanupTask(int taskId)
{
    auto conn = ConnectionPool::getInstance().getConnection();
    if (!conn) {
        return;
    }

    // 查到期任务（status=0 且退避已到期）
    MYSQL_RES* res = conn->query(
        "SELECT node_ip, node_port, file_id, retry_count FROM cleanup_queue WHERE id=" +
        std::to_string(taskId) + " AND status=0 AND next_retry_at <= NOW()");
    if (res == nullptr || mysql_num_rows(res) == 0) {
        if (res != nullptr) {
            mysql_free_result(res);
        }
        return;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    std::string ip = row[0];
    int port = std::stoi(row[1]);
    int fileId = std::stoi(row[2]);
    int retryCount = std::stoi(row[3]);
    mysql_free_result(res);

    // 直连存储节点删数据文件
    MprpcChannel channel(ip, static_cast<uint16_t>(port));
    filestore::StorageServiceRpc_Stub stub(&channel);
    filestore::DeleteFileRequest dreq;
    dreq.set_file_id(fileId);
    filestore::DeleteFileResponse dresp;
    MprpcController dctl;
    stub.DeleteFile(&dctl, &dreq, &dresp, nullptr);
    bool ok = !dctl.Failed() && dresp.result().errcode() == 0;

    if (ok) {
        // 成功：标记完成
        conn->update("UPDATE cleanup_queue SET status=1 WHERE id=" + std::to_string(taskId));
        std::cerr << "[meta] cleanup task done id:" << taskId << " file_id:" << fileId << std::endl;
    } else if (retryCount + 1 >= kMaxRetry) {
        // 达重试上限：进入终态 status=3（需人工）
        conn->update("UPDATE cleanup_queue SET status=3, retry_count=retry_count+1 WHERE id=" + std::to_string(taskId));
        std::cerr << "[ERROR] cleanup task id:" << taskId << " file_id:" << fileId
                  << " exceeded max retry, need manual" << std::endl;
    } else {
        // 指数退避：60 * 2^retry_count 秒后重试
        int backoff = 60 * (1 << retryCount);
        conn->update(
            "UPDATE cleanup_queue SET retry_count=retry_count+1, "
            "next_retry_at=DATE_ADD(NOW(), INTERVAL " + std::to_string(backoff) + " SECOND) WHERE id=" +
            std::to_string(taskId));
    }
}

// 消费线程：Redis 消费者组拉任务 ID（先重领 PEL 再读新），处理并 XACK
void MetaService::cleanupQueueLoop()
{
    while (true) {
        // 1. 先重领本消费者 PEL 未确认消息（崩溃恢复）
        auto pending = m_redis.xreadGroup(kCleanupStream, kCleanupGroup, kCleanupConsumer, "0", 100);
        for (const auto& e : pending) {
            processCleanupTask(e.second);
            m_redis.xack(kCleanupStream, kCleanupGroup, e.first);
        }
        // 2. 再读新消息
        auto fresh = m_redis.xreadGroup(kCleanupStream, kCleanupGroup, kCleanupConsumer, ">", 100);
        for (const auto& e : fresh) {
            processCleanupTask(e.second);
            m_redis.xack(kCleanupStream, kCleanupGroup, e.first);
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

// 退避线程：低频从 MySQL 捞「到期且待清理」的任务直接处理（next_retry_at 控制重试时机）
void MetaService::cleanupRetryLoop()
{
    while (true) {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (conn) {
            // 捞「到期且待清理」的任务
            MYSQL_RES* res = conn->query(
                "SELECT id FROM cleanup_queue WHERE status=0 AND next_retry_at <= NOW()");
            if (res != nullptr) {
                MYSQL_ROW row;
                std::vector<int> dueIds;
                while ((row = mysql_fetch_row(res)) != nullptr) {
                    dueIds.push_back(std::stoi(row[0]));
                }
                mysql_free_result(res);
                for (int id : dueIds) {
                    processCleanupTask(id);
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(60));
    }
}

// 分片扫描线程：每轮扫一个分片，K 轮覆盖全量（兜底清理未跟踪的孤儿块）
void MetaService::shardScanLoop()
{
    int shard = 0;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    while (true) {
        scanShard(shard % kShardCount);
        ++shard;
        std::this_thread::sleep_for(std::chrono::seconds(60));
    }
}

// 扫描一个分片：取该分片在册 file_id -> 逐节点 ListFiles(shard) -> 删孤儿（跳过队列在管的）
void MetaService::scanShard(int shard)
{
    std::vector<StorageNode> nodes;
    {
        std::lock_guard<std::mutex> lock(m_ringMutex);
        nodes = m_nodes;
    }

    // 取该分片内的在册 file_id（hash(file_id) % K == shard）
    std::set<int> knownIds;
    {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (conn) {
            MYSQL_RES* res = conn->query("SELECT id FROM file_meta");
            if (res != nullptr) {
                MYSQL_ROW row;
                while ((row = mysql_fetch_row(res)) != nullptr) {
                    int fid = std::stoi(row[0]);
                    if (static_cast<int>(ConsistentHash::hashKey(std::to_string(fid)) % static_cast<uint32_t>(kShardCount)) == shard) {
                        knownIds.insert(fid);
                    }
                }
                mysql_free_result(res);
            }
        }
    }

    // 取队列在管的 file_id（无论待清理/终态，扫描都不碰）
    std::set<int> queuedIds;
    {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (conn) {
            MYSQL_RES* res = conn->query("SELECT file_id FROM cleanup_queue");
            if (res != nullptr) {
                MYSQL_ROW row;
                while ((row = mysql_fetch_row(res)) != nullptr) {
                    queuedIds.insert(std::stoi(row[0]));
                }
                mysql_free_result(res);
            }
        }
    }

    for (const StorageNode& node : nodes) {
        MprpcChannel channel(node.ip, static_cast<uint16_t>(node.port));
        filestore::StorageServiceRpc_Stub stub(&channel);

        filestore::ListFilesRequest lreq;
        lreq.set_shard(shard);
        lreq.set_shard_count(kShardCount);
        filestore::ListFilesResponse lresp;
        MprpcController lctl;
        stub.ListFiles(&lctl, &lreq, &lresp, nullptr);
        if (lctl.Failed() || lresp.result().errcode() != 0) {
            continue;
        }

        // 逐节点 ListFiles，删「不在 file_meta 且不在队列」的孤儿
        for (const std::string& name : lresp.filenames()) {
            int fid = std::stoi(name);
            if (knownIds.find(fid) == knownIds.end() && queuedIds.find(fid) == queuedIds.end()) {
                filestore::DeleteFileRequest dreq;
                dreq.set_file_id(fid);
                filestore::DeleteFileResponse dresp;
                MprpcController dctl;
                stub.DeleteFile(&dctl, &dreq, &dresp, nullptr);
                if (dctl.Failed() || dresp.result().errcode() != 0) {
                    enqueueCleanup(node.ip, node.port, fid);
                } else {
                    std::cerr << "[meta] gc remove orphan file_id:" << fid
                              << " on " << node.ip << ":" << node.port << std::endl;
                }
            }
        }
    }
}
