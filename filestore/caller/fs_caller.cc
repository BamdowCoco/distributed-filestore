#include <iostream>
#include <fstream>
#include <string>
#include <set>
#include <vector>
#include <map>
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

// 批量上传同一节点的多个块（复用一条连接），失败重试；成功返回 true
static bool putChunksBatchWithRetry(const std::string& ip, uint16_t port,
                                    const filestore::PutChunksBatchRequest& breq,
                                    filestore::PutChunksBatchResponse& bresp)
{
    MprpcChannel channel(ip, port);
    filestore::StorageServiceRpc_Stub stub(&channel);

    for (int attempt = 1; attempt <= MAX_RETRY; ++attempt) {
        bresp.Clear();
        MprpcController ctl;
        stub.PutChunksBatch(&ctl, &breq, &bresp, nullptr);
        if (!ctl.Failed() && bresp.result().errcode() == 0) {
            return true;
        }

        std::cerr << "put chunks batch to " << ip << ":" << port << " attempt "
                  << attempt << "/" << MAX_RETRY << " failed";
        if (ctl.Failed()) {
            std::cerr << ": " << ctl.ErrorText();
        } else {
            std::cerr << ": " << bresp.result().errmsg();
        }
        std::cerr << std::endl;
        if (attempt < MAX_RETRY) {
            usleep(RETRY_INTERVAL_US);
        }
    }
    return false;
}

// 批量下载同一节点的多个块（一次 RPC），失败重试；成功返回 true
static bool getChunksBatchWithRetry(const std::string& ip, uint16_t port,
                                    const filestore::GetChunksBatchRequest& breq,
                                    filestore::GetChunksBatchResponse& bresp)
{
    MprpcChannel channel(ip, port);
    filestore::StorageServiceRpc_Stub stub(&channel);

    for (int attempt = 1; attempt <= MAX_RETRY; ++attempt) {
        bresp.Clear();
        MprpcController ctl;
        stub.GetChunksBatch(&ctl, &breq, &bresp, nullptr);
        if (!ctl.Failed() && bresp.result().errcode() == 0) {
            return true;
        }

        std::cerr << "get chunks batch from " << ip << ":" << port << " attempt "
                  << attempt << "/" << MAX_RETRY << " failed";
        if (ctl.Failed()) {
            std::cerr << ": " << ctl.ErrorText();
        } else {
            std::cerr << ": " << bresp.result().errmsg();
        }
        std::cerr << std::endl;
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

    // 每块结果：offset / size / checksum（CommitUpload 时登记）
    std::vector<int64_t> offsets(chunkCount, 0);
    std::vector<int32_t> sizes(chunkCount, 0);
    std::vector<std::string> checksums(chunkCount);

    // 2. 按「目标节点」分组块，同一节点一次批量上传（复用一条连接）
    std::map<std::string, std::vector<int>> nodeToChunks;
    for (int i = 0; i < chunkCount; ++i) {
        const filestore::ChunkLocation& loc = response.chunks(i);
        nodeToChunks[loc.ip() + ":" + std::to_string(loc.port())].push_back(i);
    }

    for (const auto& entry : nodeToChunks) {
        const std::string& node = entry.first;
        const std::vector<int>& chunkIdxs = entry.second;

        size_t colon = node.find(':');
        std::string ip = node.substr(0, colon);
        uint16_t port = static_cast<uint16_t>(std::stoi(node.substr(colon + 1)));

        // 组合同节点所有块的批量请求
        filestore::PutChunksBatchRequest breq;
        breq.set_filename(remoteName);
        for (int idx : chunkIdxs) {
            int64_t fileOffset = static_cast<int64_t>(idx) * CHUNK_SIZE;
            int len = static_cast<int>(std::min<int64_t>(CHUNK_SIZE, filesize - fileOffset));
            std::vector<char> buf(len);
            in.seekg(fileOffset, std::ios::beg);
            in.read(buf.data(), len);

            filestore::ChunkData* cd = breq.add_chunks();
            cd->set_chunk_index(idx);
            cd->set_data(std::string(buf.data(), len));
        }

        filestore::PutChunksBatchResponse bresp;
        if (!putChunksBatchWithRetry(ip, port, breq, bresp)) {
            std::cerr << "put chunks batch to " << node << " failed, rolling back..." << std::endl;
            rollbackUpload(remoteName, involvedNodes, metaStub);
            return;
        }

        // 收集该节点每块结果
        for (const auto& r : bresp.chunks()) {
            offsets[r.chunk_index()] = r.offset();
            sizes[r.chunk_index()] = r.size();
            checksums[r.chunk_index()] = r.checksum();
        }
        std::cout << "uploaded " << chunkIdxs.size() << " chunk(s) -> " << node << std::endl;
    }

    // 3. 全部块成功，提交（PENDING -> COMPLETE + 登记 offset/size/checksum）
    filestore::CommitUploadRequest commitReq;
    commitReq.set_filename(remoteName);
    for (int i = 0; i < chunkCount; ++i) {
        filestore::ChunkLocation* c = commitReq.add_chunks();
        c->set_chunk_index(i);
        c->set_offset(offsets[i]);
        c->set_size(sizes[i]);
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

// 下载：元数据查块位置 -> 按节点分组批量下载 -> 校验和比对 -> 按块号回填 downloads/
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

    int chunkCount = response.chunk_count();

    mkdir("downloads", 0755);
    std::string outPath = "downloads/" + remoteFile;
    std::ofstream out(outPath, std::ios::binary);
    if (!out) {
        std::cerr << "failed to open output file: " << outPath << std::endl;
        return;
    }

    // 按「目标节点」分组块，同一节点一次批量下载
    std::map<std::string, std::vector<int>> nodeToChunks;
    for (int i = 0; i < chunkCount; ++i) {
        const filestore::ChunkLocation& loc = response.chunks(i);
        nodeToChunks[loc.ip() + ":" + std::to_string(loc.port())].push_back(i);
    }

    int64_t written = 0;
    for (const auto& entry : nodeToChunks) {
        const std::string& node = entry.first;
        const std::vector<int>& chunkIdxs = entry.second;

        size_t colon = node.find(':');
        std::string ip = node.substr(0, colon);
        uint16_t port = static_cast<uint16_t>(std::stoi(node.substr(colon + 1)));

        // 组合同节点所有块的批量下载请求
        filestore::GetChunksBatchRequest breq;
        breq.set_filename(remoteFile);
        for (int idx : chunkIdxs) {
            const filestore::ChunkLocation& loc = response.chunks(idx);
            filestore::GetChunkSpec* spec = breq.add_chunks();
            spec->set_chunk_index(idx);
            spec->set_offset(loc.offset());
            spec->set_size(loc.size());
        }

        filestore::GetChunksBatchResponse bresp;
        if (!getChunksBatchWithRetry(ip, port, breq, bresp)) {
            std::cerr << "get chunks batch from " << node << " failed" << std::endl;
            return;
        }

        // 按 chunk_index 回填到文件对应偏移，并做校验和比对
        for (const auto& d : bresp.chunks()) {
            int idx = d.chunk_index();
            const filestore::ChunkLocation& loc = response.chunks(idx);
            if (!loc.checksum().empty() && md5Hex(d.data()) != loc.checksum()) {
                std::cerr << "get chunk " << idx << " checksum mismatch" << std::endl;
                return;
            }
            out.seekp(static_cast<int64_t>(idx) * CHUNK_SIZE, std::ios::beg);
            out.write(d.data().data(), d.data().size());
            written += d.data().size();
        }
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
