#include <string>
#include <cstdio>
#include <cstdint>
#include <sys/stat.h>

#include "file_storage.pb.h"
#include "mprpc_application.h"
#include "mprpc_provider.h"
#include "logger.h"

// 文件分块大小（与客户端保持一致）
constexpr int CHUNK_SIZE = 1024;

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

    // 上传一个文件块：写到 data_dir/<filename> 的 chunk_index*CHUNK_SIZE 偏移处
    void PutChunk(::google::protobuf::RpcController* controller,
                  const ::filestore::PutChunkRequest* request,
                  ::filestore::PutChunkResponse* response,
                  ::google::protobuf::Closure* done) override
    {
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

        long offset = static_cast<long>(request->chunk_index()) * CHUNK_SIZE;
        fseek(fp, offset, SEEK_SET);
        size_t nwrite = fwrite(request->data().data(), 1, request->data().size(), fp);
        fclose(fp);

        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        LOG_INFO("put chunk file:%s idx:%d size:%zu",
                 request->filename().c_str(), request->chunk_index(), nwrite);

        done->Run();
    }

    // 下载一个文件块：从 data_dir/<filename> 的 chunk_index*CHUNK_SIZE 偏移处读取
    void GetChunk(::google::protobuf::RpcController* controller,
                  const ::filestore::GetChunkRequest* request,
                  ::filestore::GetChunkResponse* response,
                  ::google::protobuf::Closure* done) override
    {
        std::string path = m_dataDir + "/" + request->filename();

        FILE* fp = fopen(path.c_str(), "rb");
        if (fp == nullptr) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("file not found");
            done->Run();
            return;
        }

        long offset = static_cast<long>(request->chunk_index()) * CHUNK_SIZE;
        fseek(fp, offset, SEEK_SET);
        std::string buf(request->chunk_size(), '\0');
        size_t nread = fread(&buf[0], 1, request->chunk_size(), fp);
        buf.resize(nread);
        fclose(fp);

        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        response->set_data(buf);
        LOG_INFO("get chunk file:%s idx:%d size:%zu",
                 request->filename().c_str(), request->chunk_index(), nread);

        done->Run();
    }

    // 删除本节点存储的文件数据
    void DeleteFile(::google::protobuf::RpcController* controller,
                    const ::filestore::DeleteFileRequest* request,
                    ::filestore::DeleteFileResponse* response,
                    ::google::protobuf::Closure* done) override
    {
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
