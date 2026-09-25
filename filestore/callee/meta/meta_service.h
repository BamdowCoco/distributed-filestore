#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "common/consistent_hash.h"
#include "file_storage.pb.h"
#include "redis/redis.hpp"

class Connection;

// 元数据服务：MySQL 持久化 + 虚拟文件树 + 鉴权 + 一致性哈希块分配。
// 每个文件分配全局唯一 file_id（file_meta 自增主键），数据文件以 file_id 命名，避免同名误删。
class MetaService : public filestore::MetaServiceRpc
{
public:
    MetaService();

    // 鉴权

    // 注册：插入 user 表（用户名唯一，密码 SHA256 散列）
    void Register(::google::protobuf::RpcController* controller,
                  const ::filestore::RegisterRequest* request,
                  ::filestore::RegisterResponse* response,
                  ::google::protobuf::Closure* done) override;
    // 登录：校验账号密码，签发随机 token 存 Redis
    void Login(::google::protobuf::RpcController* controller,
               const ::filestore::LoginRequest* request,
               ::filestore::LoginResponse* response,
               ::google::protobuf::Closure* done) override;
    // 登出：删除 Redis 中的会话 token
    void Logout(::google::protobuf::RpcController* controller,
                const ::filestore::LogoutRequest* request,
                ::filestore::LogoutResponse* response,
                ::google::protobuf::Closure* done) override;

    // 目录

    // 建目录：父目录须已存在、同目录不重名
    void Mkdir(::google::protobuf::RpcController* controller,
               const ::filestore::MkdirRequest* request,
               ::filestore::MkdirResponse* response,
               ::google::protobuf::Closure* done) override;
    // 删目录：非递归只删空目录；recursive 递归删除子目录/文件
    void Rmdir(::google::protobuf::RpcController* controller,
               const ::filestore::RmdirRequest* request,
               ::filestore::RmdirResponse* response,
               ::google::protobuf::Closure* done) override;
    // 列目录：返回子节点（名字/类型/file_id）
    void ListDir(::google::protobuf::RpcController* controller,
                 const ::filestore::ListDirRequest* request,
                 ::filestore::ListDirResponse* response,
                 ::google::protobuf::Closure* done) override;

    // 文件

    // 上传登记：查重后分配 file_id，返回块分配方案
    void UploadFile(::google::protobuf::RpcController* controller,
                    const ::filestore::UploadFileRequest* request,
                    ::filestore::UploadFileResponse* response,
                    ::google::protobuf::Closure* done) override;
    // 提交上传：PENDING->COMPLETE，登记每块 checksum/offset/size
    void CommitUpload(::google::protobuf::RpcController* controller,
                      const ::filestore::CommitUploadRequest* request,
                      ::filestore::CommitUploadResponse* response,
                      ::google::protobuf::Closure* done) override;
    // 取消上传：删除 PENDING 状态的元数据记录（回滚）
    void CancelUpload(::google::protobuf::RpcController* controller,
                      const ::filestore::CancelUploadRequest* request,
                      ::filestore::CancelUploadResponse* response,
                      ::google::protobuf::Closure* done) override;
    // 查询文件：路径->file_id，返回块位置映射（仅 COMPLETE）
    void QueryFile(::google::protobuf::RpcController* controller,
                   const ::filestore::QueryFileRequest* request,
                   ::filestore::QueryFileResponse* response,
                   ::google::protobuf::Closure* done) override;
    // 删除文件索引：删 file_node/file_meta/file_chunk
    void DeleteFile(::google::protobuf::RpcController* controller,
                    const ::filestore::DeleteFileRequest* request,
                    ::filestore::DeleteFileResponse* response,
                    ::google::protobuf::Closure* done) override;
    // 查询文件涉及的存储节点（去重后的 ip:port）
    void GetFileNodes(::google::protobuf::RpcController* controller,
                      const ::filestore::GetFileNodesRequest* request,
                      ::filestore::GetFileNodesResponse* response,
                      ::google::protobuf::Closure* done) override;
    // 待清理任务入队：MySQL 持久化 + Redis Stream 推任务 ID
    void AddCleanupTask(::google::protobuf::RpcController* controller,
                        const ::filestore::AddCleanupTaskRequest* request,
                        ::filestore::AddCleanupTaskResponse* response,
                        ::google::protobuf::Closure* done) override;

private:
    // 路径解析结果
    struct PathNode
    {
        int id = 0;
        int fileId = 0;
        bool isDir = false;
    };

    // 鉴权：token -> user_id，失败返回 0
    int authenticate(const std::string& token);

    // 路径/节点解析辅助（借用调用方的连接，传引用避免裸指针）
    PathNode resolvePath(Connection& conn, int userId, const std::string& path);
    int resolveParentDir(Connection& conn, int userId, const std::vector<std::string>& parts);
    bool nodeExists(Connection& conn, int userId, int parentId, const std::string& name);

    // 递归删除目录：收集后代 -> 删元数据 -> 块数据入待清理队列
    void removeDirRecursive(Connection& conn, int userId, int rootDirId);

    // 建表（幂等）
    void createTablesIfNotExist();

    // 后台线程：动态发现 + 待清理队列 + 分片扫描
    void nodeWatchLoop();
    void enqueueCleanup(const std::string& ip, int port, int fileId);
    void processCleanupTask(int taskId);
    void cleanupQueueLoop();
    void cleanupRetryLoop();
    void shardScanLoop();
    void scanShard(int shard);

    std::mutex m_ringMutex;
    ConsistentHash m_ring;
    std::vector<StorageNode> m_nodes;
    Redis m_redis;
};
