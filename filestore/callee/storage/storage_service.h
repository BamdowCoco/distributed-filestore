#pragma once

#include <cstdint>
#include <ctime>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "common/ticket.h"
#include "file_storage.pb.h"

// 存储服务：负责文件块的落盘、读取与删除。
// 数据文件以 file_id 命名（data_dir/<file_id>），避免同名文件重传导致的误删。
class StorageService : public filestore::StorageServiceRpc
{
public:
    StorageService();

    // 上传一个文件块：紧凑追加写到 data_dir/<file_id> 末尾，返回 (offset, size)
    void PutChunk(::google::protobuf::RpcController* controller,
                  const ::filestore::PutChunkRequest* request,
                  ::filestore::PutChunkResponse* response,
                  ::google::protobuf::Closure* done) override;

    // 下载一个文件块：按紧凑存储记录的 (offset, size) 读取
    void GetChunk(::google::protobuf::RpcController* controller,
                  const ::filestore::GetChunkRequest* request,
                  ::filestore::GetChunkResponse* response,
                  ::google::protobuf::Closure* done) override;

    // 批量上传：一次请求写入同一节点的多个块
    void PutChunksBatch(::google::protobuf::RpcController* controller,
                        const ::filestore::PutChunksBatchRequest* request,
                        ::filestore::PutChunksBatchResponse* response,
                        ::google::protobuf::Closure* done) override;

    // 批量下载：一次请求读取同一节点的多个块（按 offset/size 定位）
    void GetChunksBatch(::google::protobuf::RpcController* controller,
                        const ::filestore::GetChunksBatchRequest* request,
                        ::filestore::GetChunksBatchResponse* response,
                        ::google::protobuf::Closure* done) override;

    // 删除本节点存储的文件数据
    void DeleteFile(::google::protobuf::RpcController* controller,
                    const ::filestore::DeleteFileRequest* request,
                    ::filestore::DeleteFileResponse* response,
                    ::google::protobuf::Closure* done) override;

    // 列出本节点 data_dir 下的文件唯一标识（孤儿块 GC 用），**按 id 区间分页**返回。
    // 数据来源是内存索引，不再 readdir；见 m_fileIds 的说明。
    void ListFiles(::google::protobuf::RpcController* controller,
                   const ::filestore::ListFilesRequest* request,
                   ::filestore::ListFilesResponse* response,
                   ::google::protobuf::Closure* done) override;

private:
    // 校验 file_id 合法性（>0）
    static bool isValidFileId(int32_t file_id);

    // 校验存储访问票据：必须由元数据服务签发、绑定本次的 file_id 与操作且未过期
    bool checkTicket(int32_t file_id, const std::string& op, const std::string& ticket) const;

    // 数据文件路径：data_dir/<file_id>
    std::string dataPath(int32_t file_id) const;

    // ---- 本节点 file_id 内存索引 ----
    //
    // 为什么能维护得起：本节点**只有三处**会改数据文件——PutChunk / PutChunksBatch
    // （创建 + 追加）、DeleteFile（删除），全部经过 dataPath(file_id)，每处加一行即可。
    //
    // 为什么需要它：GC 的 ListFiles 若每次都 opendir/readdir 整个数据目录，代价是
    // O(文件数) 的目录遍历 + 一整个字符串列表的传输；有了索引，区间查询退化成
    // lower_bound（O(log n + k)），响应里也只有区间内的 id。
    //
    // 为什么漂移是安全的：索引多出某个 id → 元数据发起一次删除，而 DeleteFile 对不存在的
    // 文件按成功返回（幂等）；索引少掉某个 id → 这一轮不回收它，下次重建即补齐。
    // 两个方向都不会误删在册文件，所以重建只是"自愈"而不是正确性依赖。
    std::set<int32_t> m_fileIds;
    // handler 跑在框架的工作线程上（rpc_worker_threads>1 时并发），索引必须加锁
    std::mutex m_fileIdsMutex;
    // 上次重建索引的时刻（见 maybeRebuildIndex）
    std::time_t m_lastIndexRebuild = 0;

    void addFileId(int32_t file_id);
    void removeFileId(int32_t file_id);
    // 一次 readdir 重建索引。**解析不了的名字不进索引**——它们也就永远不会被当成在册文件报给 GC
    void rebuildIndex();
    // 距上次重建超过 index_rebuild_sec 才真的重建（自愈路径，正常读走内存索引）
    void maybeRebuildIndex();

    std::string m_dataDir;
    // 可信票据公钥集合（Ed25519）。元数据持私钥签发，本节点只持公钥验签——
    // 因此本节点被攻破也拿不到签发能力。可配多把以支持手动轮换。
    std::vector<ticket::TicketKey> m_ticketKeys;
};
