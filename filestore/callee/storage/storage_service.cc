#include "storage_service.h"

#include <cstdio>
#include <dirent.h>
#include <sys/stat.h>

#include "common/common.h"
#include "logger.h"
#include "mprpc_application.h"

// 构造：读取 data_dir 配置，并确保数据目录存在
StorageService::StorageService()
{
    m_dataDir = MprpcApplication::getConfig().load("data_dir");
    // 确保数据目录存在（忽略已存在的错误）
    mkdir(m_dataDir.c_str(), 0755);
}

// 上传单块：紧凑追加写到 data_dir/<file_id> 末尾，返回 (offset, size, checksum)
void StorageService::PutChunk(::google::protobuf::RpcController* controller,
                              const ::filestore::PutChunkRequest* request,
                              ::filestore::PutChunkResponse* response,
                              ::google::protobuf::Closure* done)
{
    if (!isValidFileId(request->file_id())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid file_id");
        done->Run();
        return;
    }

    std::string path = dataPath(request->file_id());

    FILE* fp = fopen(path.c_str(), "r+b");
    if (fp == nullptr) {
        // 首次写入，创建文件
        fp = fopen(path.c_str(), "wb");
    }
    if (fp == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("failed to open file for write");
        done->Run();
        return;
    }

    // 定位到文件末尾，紧凑追加写
    fseek(fp, 0, SEEK_END);
    int64_t offset = static_cast<int64_t>(ftell(fp));
    int32_t size = static_cast<int32_t>(request->data().size());
    fwrite(request->data().data(), 1, request->data().size(), fp);
    fclose(fp);

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_checksum(md5Hex(request->data()));
    response->set_offset(offset);
    response->set_size(size);
    LOG_INFO("put chunk file_id:%d idx:%d offset:%lld size:%d checksum:%s",
             request->file_id(), request->chunk_index(),
             static_cast<long long>(offset), size, response->checksum().c_str());

    done->Run();
}

// 下载单块：按紧凑存储记录的 (offset, size) 定位读取
void StorageService::GetChunk(::google::protobuf::RpcController* controller,
                              const ::filestore::GetChunkRequest* request,
                              ::filestore::GetChunkResponse* response,
                              ::google::protobuf::Closure* done)
{
    if (!isValidFileId(request->file_id())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid file_id");
        done->Run();
        return;
    }

    std::string path = dataPath(request->file_id());

    FILE* fp = fopen(path.c_str(), "rb");
    if (fp == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("file not found");
        done->Run();
        return;
    }

    fseek(fp, static_cast<long>(request->offset()), SEEK_SET);
    std::string buf(request->size(), '\0');
    size_t nread = fread(&buf[0], 1, request->size(), fp);
    buf.resize(nread);
    fclose(fp);

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_data(buf);
    LOG_INFO("get chunk file_id:%d idx:%d offset:%lld size:%zu",
             request->file_id(), request->chunk_index(),
             static_cast<long long>(request->offset()), nread);

    done->Run();
}

// 批量上传：一次请求紧凑追加写同节点多块，逐块返回 (offset, size, checksum)
void StorageService::PutChunksBatch(::google::protobuf::RpcController* controller,
                                    const ::filestore::PutChunksBatchRequest* request,
                                    ::filestore::PutChunksBatchResponse* response,
                                    ::google::protobuf::Closure* done)
{
    if (!isValidFileId(request->file_id())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid file_id");
        done->Run();
        return;
    }

    std::string path = dataPath(request->file_id());

    FILE* fp = fopen(path.c_str(), "r+b");
    if (fp == nullptr) {
        fp = fopen(path.c_str(), "wb");
    }
    if (fp == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("failed to open file for write");
        done->Run();
        return;
    }

    // 顺序紧凑追加写，逐块返回 (offset, size, checksum)
    fseek(fp, 0, SEEK_END);
    for (int i = 0; i < request->chunks_size(); ++i) {
        const auto& chunk = request->chunks(i);
        int64_t offset = static_cast<int64_t>(ftell(fp));
        int32_t size = static_cast<int32_t>(chunk.data().size());
        fwrite(chunk.data().data(), 1, chunk.data().size(), fp);

        filestore::ChunkResult* r = response->add_chunks();
        r->set_chunk_index(chunk.chunk_index());
        r->set_offset(offset);
        r->set_size(size);
        r->set_checksum(md5Hex(chunk.data()));
    }
    fclose(fp);

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    LOG_INFO("put chunks batch file_id:%d count:%d", request->file_id(), request->chunks_size());

    done->Run();
}

// 批量下载：按 (offset, size) 定位读取同节点多块
void StorageService::GetChunksBatch(::google::protobuf::RpcController* controller,
                                    const ::filestore::GetChunksBatchRequest* request,
                                    ::filestore::GetChunksBatchResponse* response,
                                    ::google::protobuf::Closure* done)
{
    if (!isValidFileId(request->file_id())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid file_id");
        done->Run();
        return;
    }

    std::string path = dataPath(request->file_id());

    FILE* fp = fopen(path.c_str(), "rb");
    if (fp == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("file not found");
        done->Run();
        return;
    }

    // 逐块按 (offset, size) 定位读取
    for (int i = 0; i < request->chunks_size(); ++i) {
        const auto& spec = request->chunks(i);
        fseek(fp, static_cast<long>(spec.offset()), SEEK_SET);
        std::string buf(spec.size(), '\0');
        size_t nread = fread(&buf[0], 1, spec.size(), fp);
        buf.resize(nread);

        filestore::GetChunkData* d = response->add_chunks();
        d->set_chunk_index(spec.chunk_index());
        d->set_data(buf);
    }
    fclose(fp);

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    LOG_INFO("get chunks batch file_id:%d count:%d", request->file_id(), request->chunks_size());

    done->Run();
}

// 删除本节点存储的文件数据（data_dir/<file_id>）
void StorageService::DeleteFile(::google::protobuf::RpcController* controller,
                                const ::filestore::DeleteFileRequest* request,
                                ::filestore::DeleteFileResponse* response,
                                ::google::protobuf::Closure* done)
{
    if (!isValidFileId(request->file_id())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid file_id");
        done->Run();
        return;
    }

    std::string path = dataPath(request->file_id());
    if (std::remove(path.c_str()) == 0) {
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        LOG_INFO("delete file_id:%d", request->file_id());
    } else {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("file not found or remove failed");
    }
    done->Run();
}

// 列出 data_dir 下的 file_id（按分片过滤，供孤儿块 GC 扫描）
void StorageService::ListFiles(::google::protobuf::RpcController* controller,
                               const ::filestore::ListFilesRequest* request,
                               ::filestore::ListFilesResponse* response,
                               ::google::protobuf::Closure* done)
{
    int shard = request->shard();
    int shardCount = request->shard_count();

    DIR* dir = opendir(m_dataDir.c_str());
    if (dir == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("failed to open data dir");
        done->Run();
        return;
    }

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        // 跳过 . / .. / 隐藏文件（数据文件都是纯数字 file_id）
        if (entry->d_name[0] == '.') {
            continue;
        }
        // 分片过滤：只返回本分片的文件（分片哈希与 ConsistentHash::hashKey 同算法）
        if (shardCount > 1) {
            uint32_t h = static_cast<uint32_t>(std::stoul(md5Hex(entry->d_name).substr(0, 8), nullptr, 16));
            if (static_cast<int>(h % static_cast<uint32_t>(shardCount)) != shard) {
                continue;
            }
        }
        response->add_filenames(entry->d_name);
    }
    closedir(dir);

    done->Run();
}

// 校验 file_id 合法性（>0）
bool StorageService::isValidFileId(int32_t file_id)
{
    return file_id > 0;
}

// 数据文件路径：data_dir/<file_id>
std::string StorageService::dataPath(int32_t file_id) const
{
    return m_dataDir + "/" + std::to_string(file_id);
}
