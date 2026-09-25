#pragma once

#include <cstdint>
#include <string>

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

    // 列出本节点 data_dir 下的文件唯一标识（孤儿块 GC 用，按分片过滤）
    void ListFiles(::google::protobuf::RpcController* controller,
                   const ::filestore::ListFilesRequest* request,
                   ::filestore::ListFilesResponse* response,
                   ::google::protobuf::Closure* done) override;

private:
    // 校验 file_id 合法性（>0）
    static bool isValidFileId(int32_t file_id);

    // 数据文件路径：data_dir/<file_id>
    std::string dataPath(int32_t file_id) const;

    std::string m_dataDir;
};
