#include "fs_client.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "common/common.h"
#include "mprpc_controller.h"

// 节点操作最大重试次数（P9）
constexpr int MAX_RETRY = 3;
// 重试间隔，单位微秒（100ms）
constexpr useconds_t RETRY_INTERVAL_US = 100 * 1000;
// 单批最大块数：60MB 载荷 / 每块大小，留 ~4MB 余量避开框架 64MB 请求/响应上限
constexpr int MAX_BATCH_CHUNKS = (60 * 1024 * 1024) / CHUNK_SIZE;

namespace {

// ---- 本地状态（token / 当前目录，存 ~/.fscli/） ----

std::string homeDir()
{
    const char* h = getenv("HOME");
    if (h != nullptr && *h != '\0') {
        return h;
    }
    struct passwd* pw = getpwuid(getuid());
    return (pw != nullptr) ? pw->pw_dir : ".";
}

std::string fscliDir()
{
    return homeDir() + "/.fscli";
}

std::string readTextFile(const std::string& path)
{
    std::ifstream in(path);
    if (!in) {
        return "";
    }
    std::string content;
    std::getline(in, content);
    return content;
}

void writeTextFile(const std::string& path, const std::string& content)
{
    mkdir(fscliDir().c_str(), 0755);
    std::ofstream out(path, std::ios::trunc);
    if (out) {
        out << content;
    }
}

std::string loadToken()
{
    return readTextFile(fscliDir() + "/token");
}

void saveToken(const std::string& token)
{
    writeTextFile(fscliDir() + "/token", token);
}

std::string loadCwd()
{
    std::string cwd = readTextFile(fscliDir() + "/cwd");
    return cwd.empty() ? "/" : cwd;
}

void saveCwd(const std::string& cwd)
{
    writeTextFile(fscliDir() + "/cwd", cwd);
}

// 相对路径 -> 绝对虚拟路径
std::string resolveVirtualPath(const std::string& cwd, const std::string& input)
{
    if (input.empty()) {
        return cwd;
    }
    if (input[0] == '/') {
        return input;
    }
    if (cwd == "/") {
        return "/" + input;
    }
    return cwd + "/" + input;
}

// 提取虚拟路径的叶子名
std::string leafName(const std::string& path)
{
    size_t pos = path.find_last_of('/');
    return (pos == std::string::npos) ? path : path.substr(pos + 1);
}

}  // namespace

// 构造：创建元数据服务 stub（走 ZooKeeper 发现模式）
FsClient::FsClient() : m_metaStub(&m_metaChannel)
{
}

// ===================== 鉴权 =====================

// 注册：调用 meta Register 创建账号
bool FsClient::registerUser(const std::string& username, const std::string& password)
{
    filestore::RegisterRequest req;
    req.set_username(username);
    req.set_password(password);
    filestore::RegisterResponse resp;
    MprpcController ctl;
    m_metaStub.Register(&ctl, &req, &resp, nullptr);
    if (ctl.Failed() || resp.result().errcode() != 0) {
        std::cerr << "register failed: "
                  << (ctl.Failed() ? ctl.ErrorText() : resp.result().errmsg()) << std::endl;
        return false;
    }
    return true;
}

// 登录：调用 meta Login，成功后本地保存 token（已登录则提示不重复签发）
bool FsClient::login(const std::string& username, const std::string& password)
{
    // 已登录则提示，不重复签发 token
    if (!loadToken().empty()) {
        std::cerr << "already logged in" << std::endl;
        return false;
    }

    filestore::LoginRequest req;
    req.set_username(username);
    req.set_password(password);
    filestore::LoginResponse resp;
    MprpcController ctl;
    m_metaStub.Login(&ctl, &req, &resp, nullptr);
    if (ctl.Failed() || resp.result().errcode() != 0) {
        std::cerr << "login failed: "
                  << (ctl.Failed() ? ctl.ErrorText() : resp.result().errmsg()) << std::endl;
        return false;
    }
    saveToken(resp.token());
    return true;
}

// 登出：调用 meta Logout 并清除本地 token（未登录则提示）
bool FsClient::logout()
{
    std::string token = loadToken();
    if (token.empty()) {
        std::cerr << "not logged in" << std::endl;
        return false;
    }

    filestore::LogoutRequest req;
    req.set_token(token);
    filestore::LogoutResponse resp;
    MprpcController ctl;
    m_metaStub.Logout(&ctl, &req, &resp, nullptr);
    saveToken("");
    return true;
}

// ===================== 目录 =====================

// 建目录：调用 meta Mkdir（path 可为相对/绝对，内部解析为绝对路径）
bool FsClient::mkdir(const std::string& path)
{
    filestore::MkdirRequest req;
    req.set_token(loadToken());
    req.set_path(resolvePath(path));
    filestore::MkdirResponse resp;
    MprpcController ctl;
    m_metaStub.Mkdir(&ctl, &req, &resp, nullptr);
    if (ctl.Failed() || resp.result().errcode() != 0) {
        std::cerr << "mkdir failed: "
                  << (ctl.Failed() ? ctl.ErrorText() : resp.result().errmsg()) << std::endl;
        return false;
    }
    return true;
}

// 删目录：调用 meta Rmdir（recursive 控制是否递归删除子目录/文件）
bool FsClient::rmdir(const std::string& path, bool recursive)
{
    filestore::RmdirRequest req;
    req.set_token(loadToken());
    req.set_path(resolvePath(path));
    req.set_recursive(recursive);
    filestore::RmdirResponse resp;
    MprpcController ctl;
    m_metaStub.Rmdir(&ctl, &req, &resp, nullptr);
    if (ctl.Failed() || resp.result().errcode() != 0) {
        std::cerr << "rmdir failed: "
                  << (ctl.Failed() ? ctl.ErrorText() : resp.result().errmsg()) << std::endl;
        return false;
    }
    return true;
}

// 列目录：调用 meta ListDir 并打印条目（目录 d / 文件 -）
bool FsClient::listDir(const std::string& path)
{
    filestore::ListDirRequest req;
    req.set_token(loadToken());
    req.set_path(resolvePath(path));
    filestore::ListDirResponse resp;
    MprpcController ctl;
    m_metaStub.ListDir(&ctl, &req, &resp, nullptr);
    if (ctl.Failed() || resp.result().errcode() != 0) {
        std::cerr << "ls failed: "
                  << (ctl.Failed() ? ctl.ErrorText() : resp.result().errmsg()) << std::endl;
        return false;
    }
    for (const auto& e : resp.entries()) {
        std::cout << (e.is_dir() ? "d " : "- ") << e.name() << std::endl;
    }
    return true;
}

// 切换当前目录：校验目标目录存在后保存本地 cwd
bool FsClient::changeDir(const std::string& path)
{
    std::string absPath = resolvePath(path);

    // 校验目标目录存在（静默调 ListDir，只看 errcode、不打印内容）
    filestore::ListDirRequest req;
    req.set_token(loadToken());
    req.set_path(absPath);
    filestore::ListDirResponse resp;
    MprpcController ctl;
    m_metaStub.ListDir(&ctl, &req, &resp, nullptr);
    if (ctl.Failed() || resp.result().errcode() != 0) {
        std::cerr << "cd failed: dir not found" << std::endl;
        return false;
    }

    saveCwd(absPath);
    return true;
}

// ===================== 文件 =====================

// 上传：登记(分配 file_id) -> 按节点分组子批上传 -> 提交（失败回滚）
bool FsClient::upload(const std::string& localFile, const std::string& virtualPath)
{
    std::ifstream in(localFile, std::ios::binary | std::ios::ate);
    if (!in) {
        std::cerr << "failed to open local file: " << localFile << std::endl;
        return false;
    }
    int64_t filesize = in.tellg();
    in.seekg(0, std::ios::beg);
    int chunkCount = static_cast<int>((filesize + CHUNK_SIZE - 1) / CHUNK_SIZE);

    filestore::UploadFileRequest request;
    request.set_token(loadToken());
    request.set_path(resolvePath(virtualPath));
    request.set_filesize(filesize);
    request.set_chunk_count(chunkCount);

    filestore::UploadFileResponse response;
    MprpcController controller;
    m_metaStub.UploadFile(&controller, &request, &response, nullptr);
    if (controller.Failed() || response.result().errcode() != 0) {
        std::cerr << "upload register failed: "
                  << (controller.Failed() ? controller.ErrorText() : response.result().errmsg()) << std::endl;
        return false;
    }
    int32_t fileId = response.file_id();

    std::set<std::string> involvedNodes;
    for (int i = 0; i < chunkCount; ++i) {
        const filestore::ChunkLocation& loc = response.chunks(i);
        involvedNodes.insert(loc.ip() + ":" + std::to_string(loc.port()));
    }

    std::vector<int64_t> offsets(chunkCount, 0);
    std::vector<int32_t> sizes(chunkCount, 0);
    std::vector<std::string> checksums(chunkCount);

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

        MprpcChannel channel(ip, port, /*sessionReuse=*/true);
        for (size_t start = 0; start < chunkIdxs.size(); start += MAX_BATCH_CHUNKS) {
            size_t end = std::min(start + static_cast<size_t>(MAX_BATCH_CHUNKS), chunkIdxs.size());
            filestore::PutChunksBatchRequest breq;
            breq.set_file_id(fileId);
            for (size_t k = start; k < end; ++k) {
                int idx = chunkIdxs[k];
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
            if (!putChunksBatch(ip, port, channel, breq, bresp)) {
                std::cerr << "put chunks batch to " << node << " failed, rolling back..." << std::endl;
                rollbackUpload(fileId, involvedNodes);
                return false;
            }
            for (const auto& r : bresp.chunks()) {
                offsets[r.chunk_index()] = r.offset();
                sizes[r.chunk_index()] = r.size();
                checksums[r.chunk_index()] = r.checksum();
            }
        }
    }

    filestore::CommitUploadRequest commitReq;
    commitReq.set_token(loadToken());
    commitReq.set_file_id(fileId);
    for (int i = 0; i < chunkCount; ++i) {
        filestore::ChunkLocation* c = commitReq.add_chunks();
        c->set_chunk_index(i);
        c->set_offset(offsets[i]);
        c->set_size(sizes[i]);
        c->set_checksum(checksums[i]);
    }
    filestore::CommitUploadResponse commitResp;
    MprpcController commitCtl;
    m_metaStub.CommitUpload(&commitCtl, &commitReq, &commitResp, nullptr);
    if (commitCtl.Failed() || commitResp.result().errcode() != 0) {
        std::cerr << "commit upload failed, rolling back..." << std::endl;
        rollbackUpload(fileId, involvedNodes);
        return false;
    }

    std::cout << "uploaded " << localFile << " -> " << resolvePath(virtualPath)
              << " (" << filesize << " bytes, " << chunkCount << " chunks)" << std::endl;
    return true;
}

// 下载：查块位置 -> 按节点分组批量下载 -> 校验和比对回填（默认 ~/Downloads）
bool FsClient::download(const std::string& virtualPath, const std::string& dest)
{
    filestore::QueryFileRequest request;
    request.set_token(loadToken());
    request.set_path(resolvePath(virtualPath));
    filestore::QueryFileResponse response;
    MprpcController controller;
    m_metaStub.QueryFile(&controller, &request, &response, nullptr);
    if (controller.Failed() || response.result().errcode() != 0) {
        std::cerr << "query failed: "
                  << (controller.Failed() ? controller.ErrorText() : response.result().errmsg()) << std::endl;
        return false;
    }
    int32_t fileId = response.file_id();
    int chunkCount = response.chunk_count();

    std::string outPath = dest;
    if (outPath.empty()) {
        ::mkdir((homeDir() + "/Downloads").c_str(), 0755);
        outPath = homeDir() + "/Downloads/" + leafName(resolvePath(virtualPath));
    }
    std::ofstream out(outPath, std::ios::binary);
    if (!out) {
        std::cerr << "failed to open output file: " << outPath << std::endl;
        return false;
    }

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

        MprpcChannel channel(ip, port, /*sessionReuse=*/true);
        for (size_t start = 0; start < chunkIdxs.size(); start += MAX_BATCH_CHUNKS) {
            size_t end = std::min(start + static_cast<size_t>(MAX_BATCH_CHUNKS), chunkIdxs.size());
            filestore::GetChunksBatchRequest breq;
            breq.set_file_id(fileId);
            for (size_t k = start; k < end; ++k) {
                int idx = chunkIdxs[k];
                const filestore::ChunkLocation& loc = response.chunks(idx);
                filestore::GetChunkSpec* spec = breq.add_chunks();
                spec->set_chunk_index(idx);
                spec->set_offset(loc.offset());
                spec->set_size(loc.size());
            }
            filestore::GetChunksBatchResponse bresp;
            if (!getChunksBatch(ip, port, channel, breq, bresp)) {
                std::cerr << "get chunks batch from " << node << " failed" << std::endl;
                return false;
            }
            for (const auto& d : bresp.chunks()) {
                int idx = d.chunk_index();
                const filestore::ChunkLocation& loc = response.chunks(idx);
                if (!loc.checksum().empty() && md5Hex(d.data()) != loc.checksum()) {
                    std::cerr << "get chunk " << idx << " checksum mismatch" << std::endl;
                    return false;
                }
                out.seekp(static_cast<int64_t>(idx) * CHUNK_SIZE, std::ios::beg);
                out.write(d.data().data(), d.data().size());
                written += d.data().size();
            }
        }
    }
    out.close();
    std::cout << "downloaded " << resolvePath(virtualPath) << " -> " << outPath
              << " (" << written << " bytes)" << std::endl;
    return true;
}

// 删除：查节点 -> 删元数据 -> 逐节点删数据（失败入待清理队列）
bool FsClient::remove(const std::string& virtualPath)
{
    filestore::GetFileNodesRequest greq;
    greq.set_token(loadToken());
    greq.set_path(resolvePath(virtualPath));
    filestore::GetFileNodesResponse gresp;
    MprpcController gctl;
    m_metaStub.GetFileNodes(&gctl, &greq, &gresp, nullptr);
    if (gctl.Failed() || gresp.result().errcode() != 0) {
        std::cerr << "get file nodes failed" << std::endl;
        return false;
    }
    int32_t fileId = gresp.file_id();

    filestore::DeleteFileRequest mreq;
    mreq.set_token(loadToken());
    mreq.set_file_id(fileId);
    filestore::DeleteFileResponse mresp;
    MprpcController mctl;
    m_metaStub.DeleteFile(&mctl, &mreq, &mresp, nullptr);
    if (mctl.Failed() || mresp.result().errcode() != 0) {
        std::cerr << "delete index failed" << std::endl;
        return false;
    }

    for (const auto& node : gresp.nodes()) {
        uint16_t port = static_cast<uint16_t>(node.port());
        if (!deleteFile(node.ip(), port, fileId)) {
            std::cerr << "delete data on " << node.ip() << ":" << node.port()
                      << " failed, enqueue cleanup" << std::endl;
            filestore::AddCleanupTaskRequest creq;
            creq.set_node_ip(node.ip());
            creq.set_node_port(node.port());
            creq.set_file_id(fileId);
            filestore::AddCleanupTaskResponse cresp;
            MprpcController cctl;
            m_metaStub.AddCleanupTask(&cctl, &creq, &cresp, nullptr);
        }
    }
    std::cout << "deleted " << resolvePath(virtualPath) << std::endl;
    return true;
}

// ===================== 私有辅助 =====================

// 相对路径 -> 绝对虚拟路径（用本地 cwd）
std::string FsClient::resolvePath(const std::string& input)
{
    return resolveVirtualPath(loadCwd(), input);
}

// 批量上传同一节点多块（复用会话连接），失败重试 MAX_RETRY 次
bool FsClient::putChunksBatch(const std::string& ip, uint16_t port, MprpcChannel& channel,
                              const filestore::PutChunksBatchRequest& breq,
                              filestore::PutChunksBatchResponse& bresp)
{
    filestore::StorageServiceRpc_Stub stub(&channel);
    for (int attempt = 1; attempt <= MAX_RETRY; ++attempt) {
        bresp.Clear();
        MprpcController ctl;
        stub.PutChunksBatch(&ctl, &breq, &bresp, nullptr);
        if (!ctl.Failed() && bresp.result().errcode() == 0) {
            return true;
        }
        std::cerr << "put chunks batch to " << ip << ":" << port << " attempt "
                  << attempt << "/" << MAX_RETRY << " failed" << std::endl;
        if (attempt < MAX_RETRY) {
            usleep(RETRY_INTERVAL_US);
        }
    }
    return false;
}

// 批量下载同一节点多块（复用会话连接），失败重试 MAX_RETRY 次
bool FsClient::getChunksBatch(const std::string& ip, uint16_t port, MprpcChannel& channel,
                              const filestore::GetChunksBatchRequest& breq,
                              filestore::GetChunksBatchResponse& bresp)
{
    filestore::StorageServiceRpc_Stub stub(&channel);
    for (int attempt = 1; attempt <= MAX_RETRY; ++attempt) {
        bresp.Clear();
        MprpcController ctl;
        stub.GetChunksBatch(&ctl, &breq, &bresp, nullptr);
        if (!ctl.Failed() && bresp.result().errcode() == 0) {
            return true;
        }
        std::cerr << "get chunks batch from " << ip << ":" << port << " attempt "
                  << attempt << "/" << MAX_RETRY << " failed" << std::endl;
        if (attempt < MAX_RETRY) {
            usleep(RETRY_INTERVAL_US);
        }
    }
    return false;
}

// 删除节点上的数据文件（data_dir/<file_id>），失败重试 MAX_RETRY 次
bool FsClient::deleteFile(const std::string& ip, uint16_t port, int32_t fileId)
{
    for (int attempt = 1; attempt <= MAX_RETRY; ++attempt) {
        MprpcChannel channel(ip, port);
        filestore::StorageServiceRpc_Stub stub(&channel);
        filestore::DeleteFileRequest req;
        req.set_file_id(fileId);
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

// 上传失败回滚：删各节点已传块 + 取消元数据 PENDING 记录
void FsClient::rollbackUpload(int32_t fileId, const std::set<std::string>& nodes)
{
    for (const auto& node : nodes) {
        size_t colon = node.find(':');
        std::string ip = node.substr(0, colon);
        uint16_t port = static_cast<uint16_t>(std::stoi(node.substr(colon + 1)));
        deleteFile(ip, port, fileId);
    }
    filestore::CancelUploadRequest creq;
    creq.set_token(loadToken());
    creq.set_file_id(fileId);
    filestore::CancelUploadResponse cresp;
    MprpcController cctl;
    m_metaStub.CancelUpload(&cctl, &creq, &cresp, nullptr);
}
