#include <iostream>
#include <fstream>
#include <string>
#include <set>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <unistd.h>
#include <sys/stat.h>

#include "file_storage.pb.h"
#include "mprpc_application.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "common/common.h"

// 节点操作最大重试次数（P9）
constexpr int MAX_RETRY = 3;
// 重试间隔，单位微秒（100ms）
constexpr useconds_t RETRY_INTERVAL_US = 100 * 1000;

// 提取路径中的文件名（去掉目录前缀）
static std::string basename(const std::string& path)
{
    size_t pos = path.find_last_of('/');
    return (pos == std::string::npos) ? path : path.substr(pos + 1);
}

// 上传单个块，失败对同一节点重试；成功返回 true 并输出校验和
static bool putChunkWithRetry(const filestore::ChunkLocation& loc,
                              const std::string& filename, int chunkIndex,
                              const std::string& data, std::string& outChecksum)
{
    for (int attempt = 1; attempt <= MAX_RETRY; ++attempt) {
        MprpcChannel channel(loc.ip(), static_cast<uint16_t>(loc.port()));
        filestore::StorageServiceRpc_Stub stub(&channel);

        filestore::PutChunkRequest req;
        req.set_filename(filename);
        req.set_chunk_index(chunkIndex);
        req.set_data(data);

        filestore::PutChunkResponse resp;
        MprpcController ctl;
        stub.PutChunk(&ctl, &req, &resp, nullptr);
        if (!ctl.Failed() && resp.result().errcode() == 0) {
            outChecksum = resp.checksum();
            return true;
        }

        std::cerr << "put chunk " << chunkIndex << " attempt " << attempt
                  << "/" << MAX_RETRY << " failed";
        if (ctl.Failed()) {
            std::cerr << ": " << ctl.ErrorText();
        } else {
            std::cerr << ": " << resp.result().errmsg();
        }
        std::cerr << std::endl;
        if (attempt < MAX_RETRY) {
            usleep(RETRY_INTERVAL_US);
        }
    }
    return false;
}

// 下载单个块，失败对同一节点重试；成功返回 true 并输出块数据
static bool getChunkWithRetry(const filestore::ChunkLocation& loc,
                              const std::string& filename, int chunkIndex,
                              int chunkSize, std::string& outData)
{
    for (int attempt = 1; attempt <= MAX_RETRY; ++attempt) {
        MprpcChannel channel(loc.ip(), static_cast<uint16_t>(loc.port()));
        filestore::StorageServiceRpc_Stub stub(&channel);

        filestore::GetChunkRequest req;
        req.set_filename(filename);
        req.set_chunk_index(chunkIndex);
        req.set_chunk_size(chunkSize);

        filestore::GetChunkResponse resp;
        MprpcController ctl;
        stub.GetChunk(&ctl, &req, &resp, nullptr);
        if (!ctl.Failed() && resp.result().errcode() == 0) {
            outData = resp.data();
            return true;
        }
        if (attempt < MAX_RETRY) {
            usleep(RETRY_INTERVAL_US);
        }
    }
    return false;
}

// 删除节点上的文件数据，失败对同一节点重试
static bool deleteFileWithRetry(const std::string& ip, uint16_t port,
                                const std::string& filename)
{
    for (int attempt = 1; attempt <= MAX_RETRY; ++attempt) {
        MprpcChannel channel(ip, port);
        filestore::StorageServiceRpc_Stub stub(&channel);

        filestore::DeleteFileRequest req;
        req.set_filename(filename);
        filestore::DeleteFileResponse resp;
        MprpcController ctl;
        stub.DeleteFile(&ctl, &req, &resp, nullptr);
        if (!ctl.Failed() && resp.result().errcode() == 0) {
            return true;
        }
        if (attempt < MAX_RETRY) {
            usleep(RETRY_INTERVAL_US);
        }
    }
    return false;
}

// 上传失败回滚：清理已传块（直连各存储节点删文件）+ 取消元数据 PENDING 记录
static void rollbackUpload(const std::string& remoteName,
                           const std::set<std::string>& nodes,
                           filestore::MetaServiceRpc_Stub& metaStub)
{
    for (const auto& node : nodes) {
        size_t colon = node.find(':');
        std::string ip = node.substr(0, colon);
        uint16_t port = static_cast<uint16_t>(std::stoi(node.substr(colon + 1)));
        deleteFileWithRetry(ip, port, remoteName);
    }

    filestore::CancelUploadRequest creq;
    creq.set_filename(remoteName);
    filestore::CancelUploadResponse cresp;
    MprpcController cctl;
    metaStub.CancelUpload(&cctl, &creq, &cresp, nullptr);
}

// 上传：流式分块读取 -> 登记元数据(PENDING) -> 逐块直连 -> 提交/回滚
static void doUpload(const std::string& localFile)
{
    std::ifstream in(localFile, std::ios::binary | std::ios::ate);
    if (!in) {
        std::cerr << "failed to open local file: " << localFile << std::endl;
        return;
    }
    int64_t filesize = in.tellg();
    in.seekg(0, std::ios::beg);

    int chunkCount = static_cast<int>((filesize + CHUNK_SIZE - 1) / CHUNK_SIZE);
    std::string remoteName = basename(localFile);

    // 元数据服务（默认 channel，走 ZooKeeper 发现）
    MprpcChannel metaChannel;
    filestore::MetaServiceRpc_Stub metaStub(&metaChannel);

    // 1. 登记元数据（status=PENDING），拿到块分配方案
    filestore::UploadFileRequest request;
    request.set_filename(remoteName);
    request.set_filesize(filesize);
    request.set_chunk_count(chunkCount);

    filestore::UploadFileResponse response;
    MprpcController controller;
    metaStub.UploadFile(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        std::cerr << "upload register failed: " << controller.ErrorText() << std::endl;
        return;
    }
    if (response.result().errcode() != 0) {
        std::cerr << "upload register failed: " << response.result().errmsg() << std::endl;
        return;
    }

    // 收集涉及的去重存储节点（用于失败回滚清理）
    std::set<std::string> involvedNodes;
    for (int i = 0; i < chunkCount; ++i) {
        const filestore::ChunkLocation& loc = response.chunks(i);
        involvedNodes.insert(loc.ip() + ":" + std::to_string(loc.port()));
    }

    // 收集每块校验和（CommitUpload 时登记到元数据）
    std::vector<std::string> checksums(chunkCount);

    // 2. 逐块流式读取 + 直连上传
    std::vector<char> buf(CHUNK_SIZE);
    for (int i = 0; i < chunkCount; ++i) {
        const filestore::ChunkLocation& loc = response.chunks(i);
        int offset = i * CHUNK_SIZE;
        int len = static_cast<int>(std::min<int64_t>(CHUNK_SIZE, filesize - offset));
        in.read(buf.data(), len);
        std::string chunk(buf.data(), len);

        if (!putChunkWithRetry(loc, remoteName, i, chunk, checksums[i])) {
            std::cerr << "put chunk " << i << " failed, rolling back..." << std::endl;
            rollbackUpload(remoteName, involvedNodes, metaStub);
            return;
        }
        std::cout << "uploaded chunk " << i << "/" << chunkCount
                  << " -> " << loc.ip() << ":" << loc.port() << std::endl;
    }

    // 3. 全部块成功，提交（PENDING -> COMPLETE + 登记校验和）
    filestore::CommitUploadRequest commitReq;
    commitReq.set_filename(remoteName);
    for (int i = 0; i < chunkCount; ++i) {
        filestore::ChunkLocation* c = commitReq.add_chunks();
        c->set_chunk_index(i);
        c->set_checksum(checksums[i]);
    }

    filestore::CommitUploadResponse commitResp;
    MprpcController commitCtl;
    metaStub.CommitUpload(&commitCtl, &commitReq, &commitResp, nullptr);
    if (commitCtl.Failed() || commitResp.result().errcode() != 0) {
        std::cerr << "commit upload failed, rolling back..." << std::endl;
        rollbackUpload(remoteName, involvedNodes, metaStub);
        return;
    }

    std::cout << "upload " << localFile << " (" << filesize << " bytes, "
              << chunkCount << " chunks) done" << std::endl;
}

// 下载：元数据查块位置 -> 逐块直连下载 -> 校验和比对 -> 流式写回 downloads/
static void doDownload(const std::string& remoteFile)
{
    MprpcChannel metaChannel;
    filestore::MetaServiceRpc_Stub metaStub(&metaChannel);

    filestore::QueryFileRequest request;
    request.set_filename(remoteFile);
    filestore::QueryFileResponse response;
    MprpcController controller;
    metaStub.QueryFile(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        std::cerr << "query failed: " << controller.ErrorText() << std::endl;
        return;
    }
    if (response.result().errcode() != 0) {
        std::cerr << "query failed: " << response.result().errmsg() << std::endl;
        return;
    }

    int64_t filesize = response.filesize();
    int chunkCount = response.chunk_count();

    mkdir("downloads", 0755);
    std::string outPath = "downloads/" + remoteFile;
    std::ofstream out(outPath, std::ios::binary);
    if (!out) {
        std::cerr << "failed to open output file: " << outPath << std::endl;
        return;
    }

    int64_t written = 0;
    for (int i = 0; i < chunkCount; ++i) {
        const filestore::ChunkLocation& loc = response.chunks(i);
        int chunkSize = CHUNK_SIZE;
        if (i == chunkCount - 1) {
            chunkSize = static_cast<int>(filesize - static_cast<int64_t>(i) * CHUNK_SIZE);
        }

        std::string data;
        if (!getChunkWithRetry(loc, remoteFile, i, chunkSize, data)) {
            std::cerr << "get chunk " << i << " failed" << std::endl;
            return;
        }

        // 校验和比对（元数据未登记校验和时跳过）
        if (!loc.checksum().empty() && md5Hex(data) != loc.checksum()) {
            std::cerr << "get chunk " << i << " checksum mismatch" << std::endl;
            return;
        }

        out.write(data.data(), data.size());
        written += data.size();
    }
    out.close();

    std::cout << "download " << remoteFile << " (" << written
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
    MprpcController qctl;
    metaStub.QueryFile(&qctl, &qreq, &qresp, nullptr);
    if (qctl.Failed() || qresp.result().errcode() != 0) {
        std::cerr << "query failed" << std::endl;
        return;
    }

    // 收集涉及的唯一存储节点
    std::set<std::string> nodes;
    for (const auto& loc : qresp.chunks()) {
        nodes.insert(loc.ip() + ":" + std::to_string(loc.port()));
    }

    // 直连各存储节点删除数据
    for (const auto& node : nodes) {
        size_t colon = node.find(':');
        std::string ip = node.substr(0, colon);
        uint16_t port = static_cast<uint16_t>(std::stoi(node.substr(colon + 1)));
        if (!deleteFileWithRetry(ip, port, remoteFile)) {
            std::cerr << "delete data on " << node << " failed" << std::endl;
        }
    }

    // 删除元数据索引
    filestore::DeleteFileRequest mreq;
    mreq.set_filename(remoteFile);
    filestore::DeleteFileResponse mresp;
    MprpcController mctl;
    metaStub.DeleteFile(&mctl, &mreq, &mresp, nullptr);
    if (mctl.Failed() || mresp.result().errcode() != 0) {
        std::cerr << "delete index failed" << std::endl;
        return;
    }

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
