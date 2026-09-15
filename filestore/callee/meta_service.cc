#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <sstream>
#include <cstdint>

#include "file_storage.pb.h"
#include "mprpc_application.h"
#include "mprpc_provider.h"

// 存储节点地址
struct StorageNode
{
    std::string ip;
    int port;
};

// 文件元数据：大小、块数、每块所在存储节点
struct FileMeta
{
    int64_t filesize;
    int32_t chunk_count;
    std::vector<filestore::ChunkLocation> chunks;
};

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

// 元数据服务：维护 文件名 -> 文件元数据（块位置）的索引
class MetaService : public filestore::MetaServiceRpc
{
public:
    MetaService()
    {
        std::string nodesStr = MprpcApplication::getConfig().load("storage_nodes");
        m_nodes = parseStorageNodes(nodesStr);
    }

    // 上传前登记文件索引，按块序号取模分配块到存储节点
    void UploadFile(::google::protobuf::RpcController* controller,
                    const ::filestore::UploadFileRequest* request,
                    ::filestore::UploadFileResponse* response,
                    ::google::protobuf::Closure* done) override
    {
        if (m_nodes.empty()) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("no storage node configured");
            done->Run();
            return;
        }

        FileMeta meta;
        meta.filesize = request->filesize();
        meta.chunk_count = request->chunk_count();

        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");

        for (int i = 0; i < request->chunk_count(); ++i) {
            const StorageNode& node = m_nodes[i % m_nodes.size()];

            filestore::ChunkLocation loc;
            loc.set_chunk_index(i);
            loc.set_ip(node.ip);
            loc.set_port(node.port);
            meta.chunks.push_back(loc);

            // 填充响应：块分配方案
            filestore::ChunkLocation* respLoc = response->add_chunks();
            respLoc->set_chunk_index(i);
            respLoc->set_ip(node.ip);
            respLoc->set_port(node.port);
        }

        m_meta[request->filename()] = meta;

        std::cout << "register file: " << request->filename()
                  << " size:" << request->filesize()
                  << " chunks:" << request->chunk_count() << std::endl;

        done->Run();
    }

    // 查询文件块位置映射
    void QueryFile(::google::protobuf::RpcController* controller,
                   const ::filestore::QueryFileRequest* request,
                   ::filestore::QueryFileResponse* response,
                   ::google::protobuf::Closure* done) override
    {
        auto it = m_meta.find(request->filename());
        if (it == m_meta.end()) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("file not found");
            done->Run();
            return;
        }

        const FileMeta& meta = it->second;
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        response->set_filesize(meta.filesize);
        response->set_chunk_count(meta.chunk_count);
        for (const auto& loc : meta.chunks) {
            filestore::ChunkLocation* respLoc = response->add_chunks();
            respLoc->set_chunk_index(loc.chunk_index());
            respLoc->set_ip(loc.ip());
            respLoc->set_port(loc.port());
        }

        done->Run();
    }

    // 删除文件索引
    void DeleteFile(::google::protobuf::RpcController* controller,
                    const ::filestore::DeleteFileRequest* request,
                    ::filestore::DeleteFileResponse* response,
                    ::google::protobuf::Closure* done) override
    {
        auto it = m_meta.find(request->filename());
        if (it == m_meta.end()) {
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("file not found");
        } else {
            m_meta.erase(it);
            response->mutable_result()->set_errcode(0);
            response->mutable_result()->set_errmsg("");
            std::cout << "delete file index: " << request->filename() << std::endl;
        }
        done->Run();
    }

private:
    std::vector<StorageNode> m_nodes;
    std::unordered_map<std::string, FileMeta> m_meta;
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
