#include <iostream>
#include <fstream>
#include <string>
#include <set>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <unistd.h>
#include <sys/stat.h>

#include "file_storage.pb.h"
#include "mprpc_application.h"
#include "mprpc_channel.h"

// 文件分块大小（与存储服务保持一致）
constexpr int CHUNK_SIZE = 1024;

// 提取路径中的文件名（去掉目录前缀）
static std::string basename(const std::string& path)
{
    size_t pos = path.find_last_of('/');
    return (pos == std::string::npos) ? path : path.substr(pos + 1);
}

// 上传：读本地文件 -> 分块 -> 元数据登记拿分配方案 -> 逐块直连存储节点
static void doUpload(const std::string& localFile)
{
    std::ifstream in(localFile, std::ios::binary);
    if (!in) {
        std::cerr << "failed to open local file: " << localFile << std::endl;
        return;
    }
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    in.close();

    int64_t filesize = static_cast<int64_t>(content.size());
    int chunkCount = static_cast<int>((filesize + CHUNK_SIZE - 1) / CHUNK_SIZE);
    std::string remoteName = basename(localFile);

    // 元数据服务（默认 channel，走 ZooKeeper 发现）
    MprpcChannel metaChannel;
    filestore::MetaServiceRpc_Stub metaStub(&metaChannel);

    filestore::UploadFileRequest request;
    request.set_filename(remoteName);
    request.set_filesize(filesize);
    request.set_chunk_count(chunkCount);

    filestore::UploadFileResponse response;
    metaStub.UploadFile(nullptr, &request, &response, nullptr);
    if (response.result().errcode() != 0) {
        std::cerr << "upload register failed: " << response.result().errmsg() << std::endl;
        return;
    }

    // 逐块直连对应存储节点上传
    for (int i = 0; i < chunkCount; ++i) {
        const filestore::ChunkLocation& loc = response.chunks(i);
        int offset = i * CHUNK_SIZE;
        int len = static_cast<int>(std::min<int64_t>(CHUNK_SIZE, filesize - offset));
        std::string chunk = content.substr(offset, len);

        MprpcChannel channel(loc.ip(), static_cast<uint16_t>(loc.port()));
        filestore::StorageServiceRpc_Stub stub(&channel);

        filestore::PutChunkRequest putReq;
        putReq.set_filename(remoteName);
        putReq.set_chunk_index(i);
        putReq.set_data(chunk);

        filestore::PutChunkResponse putResp;
        stub.PutChunk(nullptr, &putReq, &putResp, nullptr);
        if (putResp.result().errcode() != 0) {
            std::cerr << "put chunk " << i << " failed: " << putResp.result().errmsg() << std::endl;
            return;
        }
        std::cout << "uploaded chunk " << i << "/" << chunkCount
                  << " -> " << loc.ip() << ":" << loc.port() << std::endl;
    }

    std::cout << "upload " << localFile << " (" << filesize << " bytes, "
              << chunkCount << " chunks) done" << std::endl;
}

// 下载：元数据查块位置 -> 逐块直连下载 -> 拼接写回 downloads/
static void doDownload(const std::string& remoteFile)
{
    MprpcChannel metaChannel;
    filestore::MetaServiceRpc_Stub metaStub(&metaChannel);

    filestore::QueryFileRequest request;
    request.set_filename(remoteFile);
    filestore::QueryFileResponse response;
    metaStub.QueryFile(nullptr, &request, &response, nullptr);
    if (response.result().errcode() != 0) {
        std::cerr << "query failed: " << response.result().errmsg() << std::endl;
        return;
    }

    int64_t filesize = response.filesize();
    int chunkCount = response.chunk_count();

    std::string content;
    content.reserve(filesize);
    for (int i = 0; i < chunkCount; ++i) {
        const filestore::ChunkLocation& loc = response.chunks(i);
        int chunkSize = CHUNK_SIZE;
        if (i == chunkCount - 1) {
            chunkSize = static_cast<int>(filesize - static_cast<int64_t>(i) * CHUNK_SIZE);
        }

        MprpcChannel channel(loc.ip(), static_cast<uint16_t>(loc.port()));
        filestore::StorageServiceRpc_Stub stub(&channel);

        filestore::GetChunkRequest getReq;
        getReq.set_filename(remoteFile);
        getReq.set_chunk_index(i);
        getReq.set_chunk_size(chunkSize);

        filestore::GetChunkResponse getResp;
        stub.GetChunk(nullptr, &getReq, &getResp, nullptr);
        if (getResp.result().errcode() != 0) {
            std::cerr << "get chunk " << i << " failed: " << getResp.result().errmsg() << std::endl;
            return;
        }
        content += getResp.data();
    }

    mkdir("downloads", 0755);
    std::string outPath = "downloads/" + remoteFile;
    std::ofstream out(outPath, std::ios::binary);
    out.write(content.data(), content.size());
    out.close();

    std::cout << "download " << remoteFile << " (" << content.size()
              << " bytes, " << chunkCount << " chunks) -> " << outPath << std::endl;
}

// 删除：直连各存储节点删数据 -> 元数据删索引
static void doDelete(const std::string& remoteFile)
{
    MprpcChannel metaChannel;
    filestore::MetaServiceRpc_Stub metaStub(&metaChannel);

    filestore::QueryFileRequest qreq;
    qreq.set_filename(remoteFile);
    filestore::QueryFileResponse qresp;
    metaStub.QueryFile(nullptr, &qreq, &qresp, nullptr);
    if (qresp.result().errcode() != 0) {
        std::cerr << "query failed: " << qresp.result().errmsg() << std::endl;
        return;
    }

    // 收集涉及的唯一存储节点（块取模分布，可能多个块在同一节点）
    std::set<std::string> nodes;
    for (const auto& loc : qresp.chunks()) {
        nodes.insert(loc.ip() + ":" + std::to_string(loc.port()));
    }

    // 直连各存储节点删除数据
    for (const auto& node : nodes) {
        size_t colon = node.find(':');
        std::string ip = node.substr(0, colon);
        uint16_t port = static_cast<uint16_t>(std::stoi(node.substr(colon + 1)));

        MprpcChannel channel(ip, port);
        filestore::StorageServiceRpc_Stub stub(&channel);

        filestore::DeleteFileRequest dreq;
        dreq.set_filename(remoteFile);
        filestore::DeleteFileResponse dresp;
        stub.DeleteFile(nullptr, &dreq, &dresp, nullptr);
    }

    // 删除元数据索引
    filestore::DeleteFileRequest mreq;
    mreq.set_filename(remoteFile);
    filestore::DeleteFileResponse mresp;
    metaStub.DeleteFile(nullptr, &mreq, &mresp, nullptr);

    std::cout << "delete " << remoteFile << " done" << std::endl;
}

int main(int argc, char** argv)
{
    MprpcApplication::init(argc, argv);

    // init 解析 -i 后，optind 指向第一个非选项参数（子命令）
    if (optind + 1 >= argc) {
        std::cerr << "usage: " << argv[0]
                  << " -i <configfile> upload|download|delete <filename>" << std::endl;
        return 1;
    }

    std::string op = argv[optind];
    std::string filename = argv[optind + 1];

    if (op == "upload") {
        doUpload(filename);
    } else if (op == "download") {
        doDownload(filename);
    } else if (op == "delete") {
        doDelete(filename);
    } else {
        std::cerr << "unknown op: " << op << std::endl;
        return 1;
    }

    return 0;
}
