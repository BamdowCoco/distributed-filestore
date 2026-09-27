#include "meta_service.h"

#include <chrono>
#include <climits>
#include <cstdint>
#include <iostream>
#include <random>
#include <set>
#include <sstream>
#include <thread>
#include <utility>

#include "common/common.h"
#include "common/password.h"
#include "common/ticket.h"
#include "database/CommonConnectionPool.hpp"
#include "database/dao/cleanup_queue_dao.h"
#include "database/dao/file_chunk_dao.h"
#include "database/dao/file_meta_dao.h"
#include "database/dao/file_node_dao.h"
#include "database/dao/schema.h"
#include "database/dao/transaction.h"
#include "database/dao/user_dao.h"
#include "logger.h"
#include "mprpc_application.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "zk_client_util.h"

// P20 待清理队列：Redis Stream 名 / 消费者组 / 消费者名
constexpr const char* kCleanupStream = "cleanup_queue";
constexpr const char* kCleanupGroup = "cleanup-group";
constexpr const char* kCleanupConsumer = "meta-cleanup";
// 孤儿块全量对账的周期（秒）。
// 早期是「每 60s 扫一个分片、1440 片轮完一轮要 24h」，但每轮都要全表读两张表、且存储端
// 每次 ListFiles 都要重走整个数据目录并逐文件名算 MD5 → 总代价 O(文件数 × 分片数)。
// 改成每周期做一次全量对账后，单轮更重但总代价降到 O(文件数)，所以周期取长一些。
constexpr int kGcIntervalSec = 600;
// 单轮删除上限：给对账设个界，避免孤儿积压时长时间占住元数据服务
constexpr int kGcMaxDeletesPerRound = 1000;
// P20 待清理任务最大重试次数，达到后进入终态 status=3（需人工）
constexpr int kMaxRetry = 10;

// 鉴权：Redis session key 前缀与 token 有效期（秒）
constexpr const char* kSessionPrefix = "session:";
constexpr int kTokenTtl = 86400;   // 1 天

// 单文件分块数上限（4MB × 25600 = 100GB）。chunk_count 由客户端给出，
// 不设上限则一次请求即可让元数据服务分配/插入上亿行。
constexpr int32_t kMaxChunkCount = 25600;

// 超时未提交的 PENDING 上传：超过该时长仍为 PENDING 视为客户端已放弃，回收其记录与块
constexpr int kPendingTtlMinutes = 60;
constexpr int kPendingReclaimIntervalSec = 300;

// 存储访问票据有效期（秒）。
// 客户端目前是「一次取票据、全程复用」（put 票据用于整次上传、get 用于整次下载），
// 没有刷新逻辑，因此 TTL **必须长于最长一次上传/下载**——实测 5GB 上传约 112s，
// 600s 留了约 5 倍余量。之所以不取更长，是因为无 TLS 时票据可被原样重放，
// 缩短 TTL 是当前最便宜的重放窗口压缩手段。
constexpr int kTicketTtlSec = 600;

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
        // 非抛异常解析：配置里写错一个端口不该让进程在启动时抛异常退出，跳过该项即可
        int port = 0;
        if (!parseNonNegativeInt(item.substr(colon + 1), port) || port <= 0 || port > 65535) {
            LOG_ERROR("invalid storage_nodes entry (expect ip:port), skipped. entry:%s", item.c_str());
            continue;
        }
        node.port = port;
        nodes.push_back(node);
    }
    return nodes;
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
    {
        // 建表与迁移（DDL 已移到 dao/schema）。拿不到连接或建表失败就拒绝启动：
        // 元数据的每个操作都要访问这几张表，带着半套 schema 跑起来只会更晚暴露问题。
        auto conn = ConnectionPool::getInstance().getConnection();
        if (!conn || !Schema::ensure(*conn)) {
            LOG_ERROR("failed to ensure database schema, aborting startup");
            exit(EXIT_FAILURE);
        }
    }

    m_redis.connect();
    m_redis.xgroupCreate(kCleanupStream, kCleanupGroup);   // 幂等创建消费者组

    std::string nodesStr = MprpcApplication::getConfig().load("storage_nodes");
    std::vector<StorageNode> seedNodes = parseStorageNodes(nodesStr);
    if (!seedNodes.empty()) {
        m_ring.build(seedNodes);
        m_nodes = seedNodes;
    }

    // 票据签发私钥：Ed25519 的 PEM 文件**路径**（不是密钥本身）。
    // 只有元数据服务持私钥；路径可用环境变量 MPRPC_TICKET_PRIVKEY 覆盖。
    std::string privPath = MprpcApplication::getConfig().load("ticket_privkey");
    const char* envPrivPath = std::getenv("MPRPC_TICKET_PRIVKEY");
    if (envPrivPath != nullptr && *envPrivPath != '\0') {
        privPath = envPrivPath;
    }
    if (privPath.empty() || !m_ticketKey.loadPrivatePem(privPath)) {
        // 没有私钥就签不出存储节点能验签的票据，服务起来也没用：显式失败退出，
        // 不要用占位值跑起来、等到每个上传请求才暴露问题。
        LOG_ERROR("storage ticket private key is not loadable; set ticket_privkey in the config "
                  "file (or MPRPC_TICKET_PRIVKEY) to an Ed25519 PEM path. path:%s",
                  privPath.c_str());
        exit(EXIT_FAILURE);
    }
    LOG_INFO("ticket signing key loaded, kid:%s", m_ticketKey.kid().c_str());

    // 后台线程一律**不 detach**：存进 m_bgThreads 以便 stop() 能 join。
    // 此前全部 detach，进程退出时它们仍在跑 -> 无法收尾，只能靠信号直接杀死。
    m_bgThreads.emplace_back([this]() { nodeWatchLoop(); });
    m_bgThreads.emplace_back([this]() { cleanupQueueLoop(); });
    m_bgThreads.emplace_back([this]() { cleanupRetryLoop(); });
    m_bgThreads.emplace_back([this]() { reconcileLoop(); });
    m_bgThreads.emplace_back([this]() { pendingReclaimLoop(); });

    // 启动时先清一次上次运行遗留的超时 PENDING 上传
    reclaimStalePending();
}

MetaService::~MetaService()
{
    // 兜底：main 忘了调 stop() 时，joinable 的 std::thread 在析构里会直接 std::terminate。
    // stop() 幂等，重复调用无副作用。
    stop();
}

// 可被打断的等待：最多等 seconds 秒；m_stopping 置位时立即唤醒并返回 true
bool MetaService::waitForStop(int seconds)
{
    std::unique_lock<std::mutex> lock(m_stopMutex);
    m_stopCv.wait_for(lock, std::chrono::seconds(seconds), [this]() { return m_stopping.load(); });
    return m_stopping.load();
}

void MetaService::stop()
{
    if (m_stopping.exchange(true)) {
        return;   // 幂等：已经停过（或正在停）
    }
    // 唤醒所有卡在 waitForStop 里的循环
    m_stopCv.notify_all();
    for (std::thread& t : m_bgThreads) {
        if (t.joinable()) {
            t.join();
        }
    }
    m_bgThreads.clear();

    // 连接池也有后台线程（生产者 + 空闲回收）：它是进程级单例且**析构被有意绕开**
    // （见 CommonConnectionPool::getInstance 的说明），不主动停就只能随进程一起消失。
    ConnectionPool::getInstance().stop();
    LOG_INFO("[meta] background threads stopped");
}

// ===================== 鉴权 =====================

// 注册：插入 user 表（用户名唯一，口令为带盐 PBKDF2 散列）
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
    UserDao users(*conn);

    // 带随机盐的 PBKDF2 散列（早期版本是裸 SHA256，可离线爆破）
    std::string hash = password::hashPassword(request->password());
    if (hash.empty()) {
        // 取随机数或派生失败：必须当作注册失败，绝不降级去存弱散列
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("hash password failed");
        done->Run();
        return;
    }

    // 查重：用户名已存在则拒绝
    if (users.existsName(request->username())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("username already exists");
        done->Run();
        return;
    }

    // 插入新用户（散列的转义由 DAO 内部无条件完成）
    if (users.insert(request->username(), hash)) {
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
    UserDao users(*conn);

    // 取出存储的散列后在 C++ 侧校验：PBKDF2 没法在 SQL 里算，而且盐是每用户不同的
    UserDao::Row userRow;
    bool idOk = users.findByName(request->username(), userRow);

    password::VerifyResult vr = idOk ? password::verifyPassword(request->password(), userRow.passwordHash)
                                     : password::VerifyResult::Mismatch;
    if (vr == password::VerifyResult::Mismatch) {
        // 与「用户名不存在」返回同一句话，避免暴露某个用户名是否存在
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid username or password");
        done->Run();
        return;
    }
    if (vr == password::VerifyResult::OkNeedsRehash) {
        // 存储值是历史遗留的无盐 SHA256（或迭代数偏低）：趁本次已拿到明文顺手升级。
        // 升级失败不影响这次登录，下次登录会再试一次。
        std::string upgraded = password::hashPassword(request->password());
        if (!upgraded.empty()) {
            users.updatePasswordHash(userRow.id, upgraded);
        }
    }

    // 签发随机 token 存 Redis（TTL 1 天）
    std::string token = generateToken();
    m_redis.set(kSessionPrefix + token, std::to_string(userRow.id), kTokenTtl);

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_token(token);
    response->set_user_id(userRow.id);
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

    // 插入目录节点（fileId 传 0 → DAO 写 NULL）
    FileNodeDao nodes(*conn);
    if (nodes.insert(userId, parentId, parts.back(), /*isDir=*/true, /*fileIdOrZero=*/0)) {
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
        if (removeDirRecursive(*conn, userId, dirId)) {
            response->mutable_result()->set_errcode(0);
            response->mutable_result()->set_errmsg("");
        } else {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("rmdir recursive failed");
        }
        done->Run();
        return;
    }

    // 非递归：只删空目录
    FileNodeDao nodes(*conn);
    std::vector<FileNodeDao::Row> children;
    nodes.listChildren(userId, dirId, children);
    if (!children.empty()) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("dir not empty");
        done->Run();
        return;
    }

    // 删除目录节点（根目录 id=0 无行可删，直接返回成功）
    if (dirId == 0 || nodes.deleteById(dirId)) {
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
    FileNodeDao nodes(*conn);
    std::vector<FileNodeDao::Row> children;
    if (!nodes.listChildren(userId, dirId, children)) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("list dir failed");
        done->Run();
        return;
    }

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    for (const auto& child : children) {
        filestore::DirEntry* e = response->add_entries();
        e->set_name(child.name);
        e->set_is_dir(child.isDir);
        e->set_file_id(child.fileId);
    }
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

    // 文件大小与分块数由客户端给出，必须自洽且有上限：
    // filesize=0 -> chunk_count=0；否则 chunk_count == ceil(filesize / CHUNK_SIZE)
    int64_t expectChunks = (request->filesize() + CHUNK_SIZE - 1) / CHUNK_SIZE;
    if (request->filesize() < 0 || request->chunk_count() < 0 ||
        request->chunk_count() != expectChunks ||
        request->chunk_count() > kMaxChunkCount) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid filesize or chunk_count");
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

    // 事务：插入 file_meta 分配 file_id、插入文件节点、逐块登记块位置。
    // Transaction 是 RAII 的：任何提前 return 都会自动 ROLLBACK——
    // 不会再出现"漏回滚"把仍挂着事务的连接还回连接池。
    Transaction tx(*conn);
    if (!tx.ok()) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("begin transaction failed");
        done->Run();
        return;
    }

    FileMetaDao metas(*conn);
    FileNodeDao nodes(*conn);
    FileChunkDao chunkRows(*conn);

    int32_t fileId = 0;
    if (!metas.insertPending(request->filesize(), request->chunk_count(), fileId)) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("insert file_meta failed");
        done->Run();
        return;
    }

    // 插入文件节点（叶子节点，关联 file_id）
    if (!nodes.insert(userId, parentId, parts.back(), /*isDir=*/false, fileId)) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("insert file_node failed");
        done->Run();
        return;
    }

    // 一致性哈希分配块（按 file_id#chunk_index 落环）。持锁期间只做计算，不做数据库 IO
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

    // 逐块插入块位置：与上面同一事务，失败则整体回滚，避免留下「有索引无块」的半残文件
    std::vector<FileChunkDao::Loc> chunkLocs;
    chunkLocs.reserve(chunks.size());
    for (const auto& c : chunks) {
        FileChunkDao::Loc l;
        l.chunkIndex = c.chunk_index();
        l.ip = c.ip();
        l.port = c.port();
        chunkLocs.push_back(l);
    }
    if (!chunkRows.insertMany(fileId, chunkLocs)) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("insert file_chunk failed");
        done->Run();
        return;
    }

    if (!tx.commit()) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("commit failed");
        done->Run();
        return;
    }

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_file_id(fileId);
    // 上传块用的票据：存储节点据此确认本次写入在授权范围内
    response->set_ticket(makeStorageTicket(userId, fileId, "put"));
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

    Transaction tx(*conn);
    if (!tx.ok()) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("begin transaction failed");
        done->Run();
        return;
    }

    // 状态机：锁定该行并校验归属（否则任意未认证者都能把他人 PENDING 文件翻成 COMPLETE）
    int status = 0;
    std::string reason;
    if (!lockOwnedFile(*conn, userId, fileId, status, reason)) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg(reason);
        done->Run();
        return;
    }
    if (status == 1) {
        // 已 COMPLETE：幂等返回成功，重复 commit（如客户端未收到响应而重试）不应报错。
        // 这里只是只读事务，交给 tx 析构回滚即可（同时释放 FOR UPDATE 行锁）。
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        done->Run();
        return;
    }

    FileChunkDao chunkRows(*conn);
    FileMetaDao metas(*conn);

    // 块记录应已由 UploadFile 在同一事务内写全；数量不符说明元数据处于中间态，拒绝提交
    int rows = 0;
    if (!chunkRows.countByFile(fileId, rows) || rows != request->chunks_size()) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("chunk count mismatch");
        done->Run();
        return;
    }

    // 逐块登记校验和/偏移/大小（DAO 内部已把 affected==0 视为失败）
    bool ok = true;
    for (int i = 0; i < request->chunks_size() && ok; ++i) {
        const auto& c = request->chunks(i);
        ok = chunkRows.updateChunkMeta(fileId, c.chunk_index(), c.checksum(), c.offset(), c.size());
    }
    if (ok) {
        ok = metas.updateStatusComplete(fileId);
    }

    if (ok && tx.commit()) {
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
    } else {
        // 未提交 → tx 析构时自动 ROLLBACK
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

    Transaction tx(*conn);
    if (!tx.ok()) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("begin transaction failed");
        done->Run();
        return;
    }

    int status = 0;
    std::string reason;
    if (!lockOwnedFile(*conn, userId, fileId, status, reason)) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg(reason);
        done->Run();
        return;
    }
    if (status == 1) {
        // 已 COMPLETE 的文件不能走取消路径：原先此处会无条件删掉 file_chunk/file_node，
        // 只留下带 status=1 的 file_meta 与目录项，下载端会拿到一个 0 字节文件。
        // （只读事务，交给 tx 析构回滚并释放行锁。）
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("file already committed");
        done->Run();
        return;
    }

    FileChunkDao chunkRows(*conn);
    FileMetaDao metas(*conn);
    FileNodeDao nodes(*conn);

    // 取该文件涉及的各存储节点。必须在删 file_chunk 之前取；客户端回滚时不再直连删块
    // （它只有上传票据，没有删除票据），已落盘的块统一交给待清理队列，失败可退避重试。
    std::vector<FileChunkDao::NodeAddr> involved;
    if (!chunkRows.distinctNodes(fileId, involved)) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("query file_chunk failed");
        done->Run();
        return;
    }

    // 仅 PENDING 状态：删除三表中的元数据记录
    bool ok = metas.deleteById(fileId);
    if (!chunkRows.deleteByFile(fileId)) {
        ok = false;
    }
    if (!nodes.deleteByFileId(fileId)) {
        ok = false;
    }

    if (ok && tx.commit()) {
        // 提交后再入队：避免在持锁事务内做 Redis I/O；若此间进程退出，
        // 残留块会由全量对账按「不在 file_meta」兜底清理
        for (const auto& node : involved) {
            enqueueCleanup(node.ip, node.port, fileId);
        }
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
    } else {
        // 未提交 → tx 析构时自动 ROLLBACK
        response->mutable_result()->set_errmsg("cancel upload failed");
    }
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
    FileMetaDao metas(*conn);
    FileChunkDao chunkRows(*conn);
    int64_t filesize = 0;
    int chunkCount = 0;
    if (!metas.findComplete(fileId, filesize, chunkCount)) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("file not found");
        done->Run();
        return;
    }

    // 查块位置映射（按 chunk_index 排序回填）
    std::vector<FileChunkDao::Row> chunks;
    if (!chunkRows.listByFile(fileId, chunks)) {
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
    // 下载块用的票据
    response->set_ticket(makeStorageTicket(userId, fileId, "get"));

    for (const FileChunkDao::Row& c : chunks) {
        filestore::ChunkLocation* loc = response->add_chunks();
        loc->set_chunk_index(c.chunkIndex);
        loc->set_ip(c.ip);
        loc->set_port(c.port);
        loc->set_checksum(c.checksum);
        loc->set_offset(c.offset);
        loc->set_size(c.size);
    }
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

    Transaction tx(*conn);
    if (!tx.ok()) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("begin transaction failed");
        done->Run();
        return;
    }

    // 锁定并校验归属：file_id 由客户端给出，不校验归属则任意登录用户可越权删除他人文件
    int status = 0;
    std::string reason;
    if (!lockOwnedFile(*conn, userId, fileId, status, reason)) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg(reason);
        done->Run();
        return;   // 未提交 → tx 析构时自动 ROLLBACK
    }

    // 事务：删 file_meta / file_chunk / file_node
    FileMetaDao metas(*conn);
    FileChunkDao chunkRows(*conn);
    FileNodeDao nodes(*conn);
    bool ok = metas.deleteById(fileId);
    if (!chunkRows.deleteByFile(fileId)) {
        ok = false;
    }
    if (!nodes.deleteByFileId(fileId)) {
        ok = false;
    }

    if (ok && tx.commit()) {
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        std::cout << "delete file index id:" << fileId << std::endl;
    } else {
        // 未提交（或提交失败）→ tx 已回滚，客户端看到的是失败但库里没有半删状态
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
    FileChunkDao chunkRows(*conn);
    std::vector<FileChunkDao::NodeAddr> nodeAddrs;
    if (!chunkRows.distinctNodes(fileId, nodeAddrs)) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("query file_chunk failed");
        done->Run();
        return;
    }

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_file_id(fileId);
    // 删除块用的票据（客户端拿到后直连各存储节点删数据）
    response->set_ticket(makeStorageTicket(userId, fileId, "del"));
    for (const FileChunkDao::NodeAddr& addr : nodeAddrs) {
        filestore::StorageNode* n = response->add_nodes();
        n->set_ip(addr.ip);
        n->set_port(addr.port);
    }
    done->Run();
}

// 待清理任务入队：删块失败后由客户端上报，MySQL 持久化 + Redis Stream 推任务 ID
void MetaService::AddCleanupTask(::google::protobuf::RpcController* controller,
                                 const ::filestore::AddCleanupTaskRequest* request,
                                 ::filestore::AddCleanupTaskResponse* response,
                                 ::google::protobuf::Closure* done)
{
    int userId = authenticate(request->token());
    if (userId == 0) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("not logged in");
        done->Run();
        return;
    }

    // 必须限制在活跃存储节点内：否则调用者可让元数据服务代替自己向任意 ip:port
    // 发起 DeleteFile（SSRF），而该请求携带的是元数据签发的合法票据。
    if (!isActiveStorageNode(request->node_ip(), request->node_port())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("unknown node");
        done->Run();
        return;
    }

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
    int userId = 0;
    if (val.empty() || !parseNonNegativeInt(val, userId)) {
        // 会话值异常一律视为未登录，不能在此抛异常（调用方在 muduo I/O 线程上）
        return 0;
    }
    return userId;
}

// 签发存储访问票据：用 Ed25519 私钥签名；存储节点用配置里的公钥按 kid 验签，
// 因此不需要任何共享密钥，也不依赖外部服务
std::string MetaService::makeStorageTicket(int userId, int32_t fileId, const std::string& op) const
{
    return ticket::makeTicket(m_ticketKey.raw(), userId, fileId, op, kTicketTtlSec);
}

// 校验 (ip, port) 是否在当前活跃存储节点集合内（AddCleanupTask 防 SSRF）
bool MetaService::isActiveStorageNode(const std::string& ip, int port)
{
    std::lock_guard<std::mutex> lock(m_ringMutex);
    for (const StorageNode& node : m_nodes) {
        if (node.port == port && node.ip == ip) {
            return true;
        }
    }
    return false;
}

// 按路径沿 parent_id 逐级查找节点，返回末尾节点的 id/file_id/isDir（不存在 id=0）
MetaService::PathNode MetaService::resolvePath(Connection& conn, int userId, const std::string& path)
{
    FileNodeDao nodes(conn);
    PathNode result;
    std::vector<std::string> parts = splitPath(path);
    int parentId = 0;
    for (const std::string& part : parts) {
        FileNodeDao::Row row;
        if (!nodes.findChild(userId, parentId, part, row)) {
            result.id = 0;   // 中途缺失 → 整体视为不存在
            return result;
        }
        result.id = row.id;
        result.isDir = row.isDir;
        result.fileId = row.fileId;
        parentId = result.id;
    }
    return result;
}

// 解析父目录：给定已切分的路径 parts（如 ["a","b","c.txt"]），
// 逐级查找除最后一段外的每一级目录（"a"、"b"），返回最后一级目录 "b" 的节点 id；
// 中途任一级目录不存在（或不是目录）则返回 -1。
int MetaService::resolveParentDir(Connection& conn, int userId, const std::vector<std::string>& parts)
{
    FileNodeDao nodes(conn);
    int parentId = 0;   // 从根目录（parent_id=0）开始逐级向下
    // 只遍历除最后一段外的所有段（跳过最后一段，如跳过 "c.txt"）
    for (size_t i = 0; i + 1 < parts.size(); ++i) {
        int id = 0;
        if (!nodes.findChildDirId(userId, parentId, parts[i], id)) {
            return -1;   // 这一级目录不存在 → 父目录路径无效
        }
        parentId = id;
    }
    return parentId;
}

// 某名字在某父目录下是否已存在（同目录去重判断）
bool MetaService::nodeExists(Connection& conn, int userId, int parentId, const std::string& name)
{
    FileNodeDao nodes(conn);
    FileNodeDao::Row row;
    return nodes.findChild(userId, parentId, name, row);
}

// 事务内锁定 file_meta 行并校验归属。
// file_meta 本身没有 user_id，归属关系存在 file_node.user_id；
// 必须先校验归属再按 file_id 操作，否则任意登录用户枚举 file_id 即可越权删改他人文件。
bool MetaService::lockOwnedFile(Connection& conn, int userId, int32_t fileId,
                                int& status, std::string& reason)
{
    // 归属与状态查询（含 FOR UPDATE 行锁）已收敛到 DAO；调用方必须已在事务中
    FileMetaDao metas(conn);
    return metas.lockOwnedStatus(fileId, userId, status, reason);
}

// 递归删除目录：BFS 收集后代 -> **事务内**删元数据 -> 提交后把块数据入待清理队列异步清理。
// 返回 false 表示未提交（已回滚），调用方应报错。
bool MetaService::removeDirRecursive(Connection& conn, int userId, int rootDirId)
{
    struct CleanupItem
    {
        std::string ip;
        int port;
        int fileId;
    };

    // 原先这一整段没有事务：多次 DELETE 中途失败会留下"目录项已删、file_meta 未删"的
    // 半删状态，且失败的文件还会被入队清理——那是把在册文件的数据删掉（N2）。
    Transaction tx(conn);
    if (!tx.ok()) {
        return false;
    }

    FileNodeDao nodes(conn);
    FileMetaDao metas(conn);
    FileChunkDao chunkRows(conn);

    // BFS 沿 parent_id 收集目录下全部后代（目录 id + 文件 file_id）
    std::vector<int> dirIds{rootDirId};
    std::vector<int> nodeIds;   // 后代 file_node id（含目录与文件）
    std::vector<int> fileIds;   // 后代文件 file_id

    for (size_t idx = 0; idx < dirIds.size(); ++idx) {
        std::vector<FileNodeDao::Row> children;
        if (!nodes.listChildren(userId, dirIds[idx], children)) {
            // 收集不全就不能删：漏掉的后代 file_meta 会永远留在库里（真孤儿），
            // 而它的块已被删 → 数据丢失。宁可整次操作失败。
            return false;
        }
        for (const FileNodeDao::Row& child : children) {
            nodeIds.push_back(child.id);
            if (child.isDir) {
                dirIds.push_back(child.id);
            } else if (child.fileId > 0) {
                fileIds.push_back(child.fileId);
            }
        }
    }

    // 收集块数据清理任务（必须在删 file_chunk 之前取）
    std::vector<CleanupItem> cleanupItems;
    for (int fileId : fileIds) {
        std::vector<FileChunkDao::NodeAddr> nodeAddrs;
        if (!chunkRows.distinctNodes(fileId, nodeAddrs)) {
            return false;
        }
        for (const FileChunkDao::NodeAddr& addr : nodeAddrs) {
            cleanupItems.push_back({addr.ip, addr.port, fileId});
        }
    }

    // 删元数据：后代 file_node + 根目录 + file_meta + file_chunk
    bool ok = true;
    for (int nodeId : nodeIds) {
        ok = nodes.deleteById(nodeId) && ok;
    }
    // 根目录 "/"（rootDirId=0）没有对应行，deleteById 影响 0 行、不算失败
    ok = nodes.deleteById(rootDirId) && ok;
    for (int fileId : fileIds) {
        ok = metas.deleteById(fileId) && ok;
        ok = chunkRows.deleteByFile(fileId) && ok;
    }

    if (!ok || !tx.commit()) {
        return false;   // 未提交 → tx 析构时自动 ROLLBACK
    }

    // 提交后再入队：若在提交前入队，一旦回滚就会留下"文件仍在册、块却被删"的任务。
    for (const auto& item : cleanupItems) {
        enqueueCleanup(item.ip, item.port, item.fileId);
    }
    return true;
}

// 后台线程：轮询 ZK 临时节点，动态维护活跃存储节点集合（P11 动态扩缩容）
void MetaService::nodeWatchLoop()
{
    ZKClient zk;
    zk.start();

    std::string serviceName(filestore::StorageServiceRpc::descriptor()->name());
    std::string nodePath = "/" + serviceName + "/PutChunk";

    while (!m_stopping.load()) {
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
                // 非抛异常解析：本函数跑在 detached 线程里，畸形子节点名一旦抛异常就会
                // **终止整个元数据进程**；ZK 里的脏数据不该有这个能力，跳过即可。
                int port = 0;
                if (!parseNonNegativeInt(host.substr(colon + 1), port) || port <= 0 || port > 65535) {
                    LOG_ERROR("invalid zk storage node name, skipped. name:%s", host.c_str());
                    continue;
                }
                node.port = port;
                nodes.push_back(node);
            }
            if (!nodes.empty()) {
                std::lock_guard<std::mutex> lock(m_ringMutex);
                m_ring.build(nodes);
                m_nodes = nodes;
                std::cerr << "[meta] refreshed storage nodes, count:" << nodes.size() << std::endl;
            }
        }
        if (waitForStop(3)) {
            break;
        }
    }
    zk.close();   // 显式关闭：本线程的句柄活到函数返回为止，不必等析构
}

// 待清理任务入队：MySQL 幂等插入 + Redis Stream 推任务 ID
void MetaService::enqueueCleanup(const std::string& ip, int port, int fileId)
{
    auto conn = ConnectionPool::getInstance().getConnection();
    if (!conn) {
        return;
    }
    CleanupQueueDao tasks(*conn);

    // MySQL 幂等插入（同任务已存在则重置为待清理）
    if (!tasks.upsert(ip, port, fileId)) {
        LOG_ERROR("enqueue cleanup task failed, ip:%s port:%d file_id:%d", ip.c_str(), port, fileId);
        return;
    }

    // 取任务 ID 推入 Redis Stream（消费线程据此处理）
    int taskId = 0;
    if (tasks.findIdByKey(ip, port, fileId, taskId)) {
        m_redis.xadd(kCleanupStream, "id", std::to_string(taskId));
    }
}

// 回收超时未提交的 PENDING 上传。
// 客户端上传中途崩溃会留下 status=0 的 file_meta、file_node 行与已落盘的块；
// 这些 file_id 在 file_meta 里「存在」，分片扫描的孤儿判定不会碰它们，于是永久残留，
// 且同名文件从此无法重新上传（UploadFile 的同目录查重只看 file_node，不看状态）。
void MetaService::reclaimStalePending()
{
    std::vector<int> staleIds;
    {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (!conn) {
            return;
        }
        FileMetaDao metas(*conn);
        if (!metas.listStalePendingIds(kPendingTtlMinutes, staleIds)) {
            return;
        }
        // 先把各存储节点的删块任务入队（失败可由队列退避重试），再删元数据；
        // 若这里没取到节点，块会成为孤儿，由全量对账兜底
        FileChunkDao chunkRows(*conn);
        for (int fileId : staleIds) {
            std::vector<FileChunkDao::NodeAddr> nodeAddrs;
            if (!chunkRows.distinctNodes(fileId, nodeAddrs)) {
                continue;
            }
            for (const FileChunkDao::NodeAddr& addr : nodeAddrs) {
                enqueueCleanup(addr.ip, addr.port, fileId);
            }
        }
    }

    if (staleIds.empty()) {
        return;
    }

    auto conn = ConnectionPool::getInstance().getConnection();
    if (!conn) {
        return;
    }
    FileMetaDao metas(*conn);
    FileChunkDao chunkRows(*conn);
    FileNodeDao nodes(*conn);
    for (int fileId : staleIds) {
        // 三条删除必须同一事务：中途失败会留下"file_meta 已删、file_node 未删"的状态，
        // 于是这个 file_id 不再是 PENDING、目录项却还在，同名文件永远无法重新上传
        Transaction tx(*conn);
        if (!tx.ok()) {
            continue;
        }
        bool ok = chunkRows.deleteByFile(fileId);
        ok = nodes.deleteByFileId(fileId) && ok;
        ok = metas.deleteById(fileId) && ok;
        if (ok && tx.commit()) {
            std::cout << "[meta] reclaim stale pending upload file_id:" << fileId << std::endl;
        }
    }
}

// 后台线程：定期回收超时 PENDING 上传
void MetaService::pendingReclaimLoop()
{
    // 先等一个周期再开工（原行为）；waitForStop 让它可被打断
    while (!waitForStop(kPendingReclaimIntervalSec)) {
        reclaimStalePending();
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
    CleanupQueueDao tasks(*conn);
    CleanupQueueDao::Task task;
    if (!tasks.findDueById(taskId, task)) {
        return;
    }

    // 直连存储节点删数据文件
    MprpcChannel channel(task.ip, static_cast<uint16_t>(task.port));
    filestore::StorageServiceRpc_Stub stub(&channel);
    filestore::DeleteFileRequest dreq;
    dreq.set_file_id(task.fileId);
    // 每次尝试都重新签发（退避最长可达小时级，不能复用上一次的票据）
    dreq.set_ticket(makeStorageTicket(0, task.fileId, "del"));
    filestore::DeleteFileResponse dresp;
    MprpcController dctl;
    stub.DeleteFile(&dctl, &dreq, &dresp, nullptr);
    bool ok = !dctl.Failed() && dresp.result().errcode() == 0;

    if (ok) {
        // 成功：标记完成
        tasks.markDone(taskId);
        std::cerr << "[meta] cleanup task done id:" << taskId << " file_id:" << task.fileId << std::endl;
    } else if (task.retryCount + 1 >= kMaxRetry) {
        // 达重试上限：进入终态 status=3（需人工）
        tasks.markManual(taskId);
        std::cerr << "[ERROR] cleanup task id:" << taskId << " file_id:" << task.fileId
                  << " exceeded max retry, need manual" << std::endl;
    } else {
        // 指数退避：60 * 2^retry_count 秒后重试（retryCount < kMaxRetry=10，不会溢出）
        tasks.markRetryBackoff(taskId, 60 * (1 << task.retryCount));
    }
}

// 消费线程：Redis 消费者组拉任务 ID（先重领 PEL 再读新），处理并 XACK
void MetaService::cleanupQueueLoop()
{
    while (!m_stopping.load()) {
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
        if (waitForStop(1)) {
            break;
        }
    }
}

// 退避线程：低频从 MySQL 捞「到期且待清理」的任务直接处理（next_retry_at 控制重试时机）
void MetaService::cleanupRetryLoop()
{
    while (!m_stopping.load()) {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (conn) {
            // 捞「到期且待清理」的任务
            CleanupQueueDao tasks(*conn);
            std::vector<int> dueIds;
            if (tasks.listDueIds(dueIds)) {
                for (int id : dueIds) {
                    processCleanupTask(id);
                }
            }
        }
        if (waitForStop(60)) {
            break;
        }
    }
}

// 孤儿块回收线程：周期性做一次**全量对账**（兜底清理未被任何索引跟踪的孤儿块）
void MetaService::reconcileLoop()
{
    // 周期可配（gc_interval_sec）：既方便按机器规模调整，也便于验证时调短
    const int intervalSec = MprpcApplication::getConfig().getPositiveInt("gc_interval_sec", kGcIntervalSec);
    // 启动后先等一会再首扫，避开与服务注册/建表的资源竞争
    if (waitForStop(5)) {
        return;
    }
    while (!m_stopping.load()) {
        reconcileOrphans();
        if (waitForStop(intervalSec)) {
            break;
        }
    }
}

// 一次全量对账：在册 file_id 与队列在管的 file_id 各读一次，逐节点列出完整文件列表，
// 删掉「两边都没有」的孤儿
void MetaService::reconcileOrphans()
{
    std::vector<StorageNode> nodes;
    {
        std::lock_guard<std::mutex> lock(m_ringMutex);
        nodes = m_nodes;
    }
    if (nodes.empty()) {
        return;
    }

    std::set<int> knownIds;    // 在册 file_id（**含 PENDING**：上传中的文件必须保护）
    std::set<int> queuedIds;   // 队列在管的 file_id（待清理或终态，对账都不碰）
    {
        auto conn = ConnectionPool::getInstance().getConnection();
        if (!conn) {
            return;   // 拿不到连接就整轮跳过，下个周期再试
        }
        std::vector<int> ids;
        FileMetaDao metas(*conn);
        if (!metas.listAllIds(ids)) {
            return;
        }
        knownIds.insert(ids.begin(), ids.end());

        CleanupQueueDao tasks(*conn);
        ids.clear();
        if (!tasks.listAllFileIds(ids)) {
            return;
        }
        queuedIds.insert(ids.begin(), ids.end());
    }

    int removed = 0;
    for (const StorageNode& node : nodes) {
        MprpcChannel channel(node.ip, static_cast<uint16_t>(node.port));
        filestore::StorageServiceRpc_Stub stub(&channel);

        filestore::ListFilesRequest lreq;
        // GC 没有用户会话，用私钥自签票据（file_id=0 表示非文件维度操作）
        lreq.set_ticket(makeStorageTicket(0, 0, "list"));
        filestore::ListFilesResponse lresp;
        MprpcController lctl;
        stub.ListFiles(&lctl, &lreq, &lresp, nullptr);
        if (lctl.Failed() || lresp.result().errcode() != 0) {
            // 必须记下来：静默 continue 会让「票据被拒 / 节点不可达」看起来和「没有孤儿」
            // 一模一样——曾因此让整条 GC 静默失效很久没被发现。
            std::cerr << "[meta] gc ListFiles failed on " << node.ip << ":" << node.port
                      << " err:" << (lctl.Failed() ? lctl.ErrorText() : lresp.result().errmsg())
                      << std::endl;
            continue;
        }

        for (const std::string& name : lresp.filenames()) {
            // 文件名来自存储节点的数据目录，可能是任意内容（调试残留、编辑器临时文件等）；
            // 这里跑在 detach 的线程里，解析必须宽松——抛异常会直接终止元数据进程
            int fid = 0;
            if (!parseNonNegativeInt(name, fid)) {
                continue;
            }
            if (knownIds.find(fid) != knownIds.end() || queuedIds.find(fid) != queuedIds.end()) {
                continue;   // 在册或队列在管，不是孤儿
            }

            filestore::DeleteFileRequest dreq;
            dreq.set_file_id(fid);
            dreq.set_ticket(makeStorageTicket(0, fid, "del"));
            filestore::DeleteFileResponse dresp;
            MprpcController dctl;
            stub.DeleteFile(&dctl, &dreq, &dresp, nullptr);
            if (dctl.Failed() || dresp.result().errcode() != 0) {
                enqueueCleanup(node.ip, node.port, fid);
            } else {
                ++removed;
                std::cerr << "[meta] gc remove orphan file_id:" << fid
                          << " on " << node.ip << ":" << node.port << std::endl;
                if (removed >= kGcMaxDeletesPerRound) {
                    // 单轮删除上限：孤儿积压时不要把元数据服务长时间占住，剩下的下轮再做
                    std::cerr << "[meta] gc hit per-round delete cap:" << kGcMaxDeletesPerRound
                              << ", remaining handled next round" << std::endl;
                    return;
                }
            }
        }
    }

    if (removed > 0) {
        std::cerr << "[meta] gc reconcile done, removed:" << removed << std::endl;
    }
}
