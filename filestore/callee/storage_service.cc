#include <string>
#include <cstdio>
#include <cstdint>
#include <sys/stat.h>
#include <dirent.h>

#include "file_storage.pb.h"
#include "mprpc_application.h"
#include "mprpc_provider.h"
#include "logger.h"
#include "common/common.h"

// 校验文件名合法性：拒绝空、路径分隔符、相对/绝对路径穿越
static bool isValidFilename(const std::string& name)
{
    if (name.empty()) {
        return false;
    }
    if (name == "." || name == "..") {
        return false;
    }
    // 绝对路径
    if (name[0] == '/') {
        return false;
    }
    // 含路径分隔符（不允许多级路径 / 穿越）
    if (name.find('/') != std::string::npos || name.find('\\') != std::string::npos) {
        return false;
    }
    return true;
}

// 存储服务：负责文件块的落盘、读取与删除
class StorageService : public filestore::StorageServiceRpc
{
public:
    StorageService()
    {
        m_dataDir = MprpcApplication::getConfig().load("data_dir");
        // 确保数据目录存在（忽略已存在的错误）
        mkdir(m_dataDir.c_str(), 0755);
    }

    // 上传一个文件块：紧凑追加写到 data_dir/<filename> 末尾，返回 (offset, size)
    void PutChunk(::google::protobuf::RpcController* controller,
                  const ::filestore::PutChunkRequest* request,
                  ::filestore::PutChunkResponse* response,
                  ::google::protobuf::Closure* done) override
    {
        if (!isValidFilename(request->filename())) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("invalid filename");
            done->Run();
            return;
        }

        std::string path = m_dataDir + "/" + request->filename();

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
        LOG_INFO("put chunk file:%s idx:%d offset:%lld size:%d checksum:%s",
                 request->filename().c_str(), request->chunk_index(),
                 static_cast<long long>(offset), size, response->checksum().c_str());

        done->Run();
    }

    // 下载一个文件块：按紧凑存储记录的 (offset, size) 读取
    void GetChunk(::google::protobuf::RpcController* controller,
                  const ::filestore::GetChunkRequest* request,
                  ::filestore::GetChunkResponse* response,
                  ::google::protobuf::Closure* done) override
    {
        if (!isValidFilename(request->filename())) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("invalid filename");
            done->Run();
            return;
        }

        std::string path = m_dataDir + "/" + request->filename();

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
        LOG_INFO("get chunk file:%s idx:%d offset:%lld size:%zu",
                 request->filename().c_str(), request->chunk_index(),
                 static_cast<long long>(request->offset()), nread);

        done->Run();
    }

    // 批量上传：一次请求写入同一节点的多个块（紧凑追加写，减少往返）
    void PutChunksBatch(::google::protobuf::RpcController* controller,
                        const ::filestore::PutChunksBatchRequest* request,
                        ::filestore::PutChunksBatchResponse* response,
                        ::google::protobuf::Closure* done) override
    {
        if (!isValidFilename(request->filename())) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("invalid filename");
            done->Run();
            return;
        }

        std::string path = m_dataDir + "/" + request->filename();

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
        LOG_INFO("put chunks batch file:%s count:%d", request->filename().c_str(), request->chunks_size());

        done->Run();
    }

    // 批量下载：一次请求读取同一节点的多个块（按 offset/size 定位）
    void GetChunksBatch(::google::protobuf::RpcController* controller,
                        const ::filestore::GetChunksBatchRequest* request,
                        ::filestore::GetChunksBatchResponse* response,
                        ::google::protobuf::Closure* done) override
    {
        if (!isValidFilename(request->filename())) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("invalid filename");
            done->Run();
            return;
        }

        std::string path = m_dataDir + "/" + request->filename();

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
        LOG_INFO("get chunks batch file:%s count:%d", request->filename().c_str(), request->chunks_size());

        done->Run();
    }

    // 删除本节点存储的文件数据
    void DeleteFile(::google::protobuf::RpcController* controller,
                    const ::filestore::DeleteFileRequest* request,
                    ::filestore::DeleteFileResponse* response,
                    ::google::protobuf::Closure* done) override
    {
        if (!isValidFilename(request->filename())) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("invalid filename");
            done->Run();
            return;
        }

        std::string path = m_dataDir + "/" + request->filename();
        if (std::remove(path.c_str()) == 0) {
            response->mutable_result()->set_errcode(0);
            response->mutable_result()->set_errmsg("");
            LOG_INFO("delete file:%s", request->filename().c_str());
        } else {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("file not found or remove failed");
        }
        done->Run();
    }

    // 列出本节点 data_dir 下的所有文件名（P20 孤儿块 GC 用）
    void ListFiles(::google::protobuf::RpcController* controller,
                   const ::filestore::ListFilesRequest* request,
                   ::filestore::ListFilesResponse* response,
                   ::google::protobuf::Closure* done) override
    {
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
            // 跳过 . / .. / 隐藏文件（数据文件都是普通文件名）
            if (entry->d_name[0] == '.') {
                continue;
            }
            response->add_filenames(entry->d_name);
        }
        closedir(dir);

        done->Run();
    }

private:
    std::string m_dataDir;
};

int main(int argc, char** argv)
{
    // 框架初始化
    MprpcApplication::init(argc, argv);

    // 注册存储服务并启动
    RpcProvider provider;
    provider.notifyService(new StorageService());
    provider.run();

    return 0;
}
