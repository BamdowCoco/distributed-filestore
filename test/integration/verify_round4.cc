// filestore 第四轮（正确性与安全）修复的针对性验证。
//
// 直接构造 RPC 请求，断言服务端的实际拒绝/接受行为——这些路径 CLI 覆盖不到
// （CLI 只能按正常流程走，构造不出「用他人 file_id」「空票据」「越界 size」这类请求）。
//
// 前置：ZooKeeper + MySQL + Redis 在跑，且已启动 1 个 meta_callee 与至少 1 个 storage_callee。
// 四个服务进程需共用同一个 MPRPC_TICKET_SECRET；本程序不需要该密钥，只负责搬运票据。
//
// 用法：./bin/fs_security_test -i config/filestore_meta.cnf
#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <dirent.h>
#include <unistd.h>

#include "common/common.h"
#include "file_storage.pb.h"
#include "mprpc_application.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"

namespace {

int g_pass = 0;
int g_fail = 0;

void check(bool cond, const std::string& what, const std::string& detail = "")
{
    if (cond) {
        ++g_pass;
        std::cout << "  ok   " << what << std::endl;
    } else {
        ++g_fail;
        std::cout << "  FAIL " << what;
        if (!detail.empty()) {
            std::cout << "   [" << detail << "]";
        }
        std::cout << std::endl;
    }
}

bool contains(const std::string& hay, const std::string& needle)
{
    return hay.find(needle) != std::string::npos;
}

// 本进程的线程数（/proc/self/task 下的条目减去 . 与 ..）——用于验证 C1 的 zhandle 泄漏已修
int threadCount()
{
    DIR* d = opendir("/proc/self/task");
    if (d == nullptr) {
        return -1;
    }
    int n = 0;
    while (readdir(d) != nullptr) {
        ++n;
    }
    closedir(d);
    return n - 2;
}

// 元数据服务 stub：走 ZooKeeper 服务发现（与 fs_client 一致，因此需要 ZK）
filestore::MetaServiceRpc_Stub& metaStub()
{
    static MprpcChannel channel;
    static filestore::MetaServiceRpc_Stub stub(&channel);
    return stub;
}

std::string registerAndLogin(const std::string& user, const std::string& pwd)
{
    {
        // 已存在会失败，忽略即可
        filestore::RegisterRequest req;
        req.set_username(user);
        req.set_password(pwd);
        filestore::RegisterResponse resp;
        MprpcController ctl;
        metaStub().Register(&ctl, &req, &resp, nullptr);
    }
    filestore::LoginRequest req;
    req.set_username(user);
    req.set_password(pwd);
    filestore::LoginResponse resp;
    MprpcController ctl;
    metaStub().Login(&ctl, &req, &resp, nullptr);
    if (ctl.Failed() || resp.result().errcode() != 0) {
        std::cerr << "登录失败 " << user << ": "
                  << (ctl.Failed() ? ctl.ErrorText() : resp.result().errmsg()) << std::endl;
        return "";
    }
    return resp.token();
}

// 上传结果：fileId + 元数据下发的票据 + 各块位置
struct Uploaded
{
    int32_t fileId = 0;
    std::string putTicket;
    std::string getTicket;
    std::string delTicket;
    int chunkCount = 0;
    std::vector<filestore::ChunkLocation> chunks;
};

// 完整走一遍「UploadFile -> PutChunksBatch -> CommitUpload」
bool uploadFile(const std::string& token, const std::string& path,
                const std::string& data, Uploaded& out, std::string& err)
{
    int chunkCount = static_cast<int>((data.size() + CHUNK_SIZE - 1) / CHUNK_SIZE);
    filestore::UploadFileRequest req;
    req.set_token(token);
    req.set_path(path);
    req.set_filesize(static_cast<int64_t>(data.size()));
    req.set_chunk_count(chunkCount);
    filestore::UploadFileResponse resp;
    MprpcController ctl;
    metaStub().UploadFile(&ctl, &req, &resp, nullptr);
    if (ctl.Failed() || resp.result().errcode() != 0) {
        err = ctl.Failed() ? ctl.ErrorText() : resp.result().errmsg();
        return false;
    }
    out.fileId = resp.file_id();
    out.putTicket = resp.ticket();
    out.chunkCount = chunkCount;
    out.chunks.clear();
    for (int i = 0; i < resp.chunks_size(); ++i) {
        out.chunks.push_back(resp.chunks(i));
    }

    // 按块所在节点分组，逐节点批量上传
    std::map<std::string, std::vector<int>> byNode;
    for (int i = 0; i < chunkCount; ++i) {
        const auto& loc = resp.chunks(i);
        byNode[loc.ip() + ":" + std::to_string(loc.port())].push_back(i);
    }

    std::vector<int64_t> offsets(chunkCount, 0);
    std::vector<int32_t> sizes(chunkCount, 0);
    std::vector<std::string> checksums(chunkCount);

    for (const auto& e : byNode) {
        size_t colon = e.first.find(':');
        std::string ip = e.first.substr(0, colon);
        uint16_t port = static_cast<uint16_t>(std::stoi(e.first.substr(colon + 1)));

        filestore::PutChunksBatchRequest breq;
        breq.set_file_id(out.fileId);
        breq.set_ticket(out.putTicket);
        for (int idx : e.second) {
            int64_t off = static_cast<int64_t>(idx) * CHUNK_SIZE;
            int len = static_cast<int>(std::min<int64_t>(CHUNK_SIZE, data.size() - off));
            filestore::ChunkData* cd = breq.add_chunks();
            cd->set_chunk_index(idx);
            cd->set_data(data.substr(off, len));
        }
        filestore::PutChunksBatchResponse bresp;
        MprpcChannel ch(ip, port);
        filestore::StorageServiceRpc_Stub stub(&ch);
        MprpcController bctl;
        stub.PutChunksBatch(&bctl, &breq, &bresp, nullptr);
        if (bctl.Failed() || bresp.result().errcode() != 0) {
            err = bctl.Failed() ? bctl.ErrorText() : bresp.result().errmsg();
            return false;
        }
        for (int i = 0; i < bresp.chunks_size(); ++i) {
            const auto& r = bresp.chunks(i);
            offsets[r.chunk_index()] = r.offset();
            sizes[r.chunk_index()] = r.size();
            checksums[r.chunk_index()] = r.checksum();
        }
    }

    filestore::CommitUploadRequest creq;
    creq.set_token(token);
    creq.set_file_id(out.fileId);
    for (int i = 0; i < chunkCount; ++i) {
        filestore::ChunkLocation* c = creq.add_chunks();
        c->set_chunk_index(i);
        c->set_offset(offsets[i]);
        c->set_size(sizes[i]);
        c->set_checksum(checksums[i]);
    }
    filestore::CommitUploadResponse cresp;
    MprpcController cctl;
    metaStub().CommitUpload(&cctl, &creq, &cresp, nullptr);
    if (cctl.Failed() || cresp.result().errcode() != 0) {
        err = cctl.Failed() ? cctl.ErrorText() : cresp.result().errmsg();
        return false;
    }
    // 注意：此处签发的是 put 票据。get/del 票据由 QueryFile / GetFileNodes 单独下发，
    // 调用方需另行获取后填进 out。
    return true;
}

// 取文件信息（含 get 票据、del 票据）
bool queryFile(const std::string& token, const std::string& path,
               filestore::QueryFileResponse& out, std::string& err)
{
    filestore::QueryFileRequest req;
    req.set_token(token);
    req.set_path(path);
    MprpcController ctl;
    metaStub().QueryFile(&ctl, &req, &out, nullptr);
    if (ctl.Failed() || out.result().errcode() != 0) {
        err = ctl.Failed() ? ctl.ErrorText() : out.result().errmsg();
        return false;
    }
    return true;
}

bool getFileNodes(const std::string& token, const std::string& path,
                  filestore::GetFileNodesResponse& out, std::string& err)
{
    filestore::GetFileNodesRequest req;
    req.set_token(token);
    req.set_path(path);
    MprpcController ctl;
    metaStub().GetFileNodes(&ctl, &req, &out, nullptr);
    if (ctl.Failed() || out.result().errcode() != 0) {
        err = ctl.Failed() ? ctl.ErrorText() : out.result().errmsg();
        return false;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv)
{
    MprpcApplication::init(argc, argv);

    std::cout << "===== filestore 第四轮修复：针对性验证 =====" << std::endl;

    std::string tokenA = registerAndLogin("sec_test_a", "sec_test_pwd");
    std::string tokenB = registerAndLogin("sec_test_b", "sec_test_pwd");
    if (tokenA.empty() || tokenB.empty()) {
        std::cerr << "两个测试账号登录失败，无法继续" << std::endl;
        return 1;
    }

    // 每次运行用不同路径，避免与上一次的残留同名冲突
    const std::string tag = std::to_string(getpid());
    const std::string pathA = "/seca_" + tag + ".bin";
    const std::string pathB = "/secb_" + tag + ".bin";
    const std::string payload(100 * 1024, 'A');   // 100KB -> 1 块

    std::string err;

    // ---------- S1: CommitUpload / CancelUpload 无鉴权 ----------
    std::cout << "\n[S1] CommitUpload/CancelUpload 缺少鉴权（修复前完全无校验）" << std::endl;
    {
        filestore::CommitUploadRequest req;
        req.set_token("");
        req.set_file_id(1);
        filestore::CommitUploadResponse resp;
        MprpcController ctl;
        metaStub().CommitUpload(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() != 0 &&
                  resp.result().errmsg() == "not logged in",
              "CommitUpload 空 token 被拒绝", resp.result().errmsg());
    }
    {
        filestore::CancelUploadRequest req;
        req.set_token("");
        req.set_file_id(1);
        filestore::CancelUploadResponse resp;
        MprpcController ctl;
        metaStub().CancelUpload(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() != 0 &&
                  resp.result().errmsg() == "not logged in",
              "CancelUpload 空 token 被拒绝", resp.result().errmsg());
    }

    // ---------- 准备：A 与 B 各上传一个文件 ----------
    Uploaded fileA;
    Uploaded fileB;
    bool okA = uploadFile(tokenA, pathA, payload, fileA, err);
    check(okA, "账号 A 上传成功（含元数据下发的 put 票据被存储节点接受）", err);
    bool okB = uploadFile(tokenB, pathB, payload, fileB, err);
    check(okB, "账号 B 上传成功", err);
    if (!okA || !okB) {
        std::cout << "\n前置上传失败，后续断言无法进行" << std::endl;
        return 1;
    }
    {
        filestore::QueryFileResponse q;
        std::string qerr;
        check(queryFile(tokenA, pathA, q, qerr), "QueryFile 取到 A 的文件与 get 票据", qerr);
        fileA.getTicket = q.ticket();
        fileA.chunkCount = q.chunk_count();
    }

    // ---------- S2: 越权删除他人文件 ----------
    std::cout << "\n[S2] 归属校验（修复前任意登录用户可删他人文件）" << std::endl;
    {
        filestore::DeleteFileRequest req;
        req.set_token(tokenB);
        req.set_file_id(fileA.fileId);   // B 拿 A 的 file_id
        filestore::DeleteFileResponse resp;
        MprpcController ctl;
        metaStub().DeleteFile(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() != 0 &&
                  contains(resp.result().errmsg(), "permission denied"),
              "B 用 A 的 file_id 调 DeleteFile 被拒", resp.result().errmsg());
    }
    {
        // 关键回归：A 的文件必须仍然完整可下载
        filestore::QueryFileResponse q;
        std::string qerr;
        bool ok = queryFile(tokenA, pathA, q, qerr);
        check(ok && q.chunk_count() == fileA.chunkCount,
              "被越权尝试后 A 的文件仍完整（QueryFile 正常）", qerr);
    }
    {
        filestore::DeleteFileRequest req;
        req.set_token(tokenB);
        req.set_file_id(fileB.fileId);   // B 删自己的
        filestore::DeleteFileResponse resp;
        MprpcController ctl;
        metaStub().DeleteFile(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() == 0,
              "B 删自己的文件成功", resp.result().errmsg());
    }

    // ---------- C3: 对不存在的 file_id 必须返回失败 ----------
    std::cout << "\n[C3] 不存在/越界的 file_id 不再静默成功" << std::endl;
    {
        filestore::CommitUploadRequest req;
        req.set_token(tokenA);
        req.set_file_id(999999999);
        filestore::CommitUploadResponse resp;
        MprpcController ctl;
        metaStub().CommitUpload(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() != 0 &&
                  contains(resp.result().errmsg(), "not found"),
              "CommitUpload 不存在的 file_id 返回失败", resp.result().errmsg());
    }
    {
        filestore::DeleteFileRequest req;
        req.set_token(tokenA);
        req.set_file_id(999999999);
        filestore::DeleteFileResponse resp;
        MprpcController ctl;
        metaStub().DeleteFile(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() != 0,
              "DeleteFile 不存在的 file_id 返回失败", resp.result().errmsg());
    }

    // ---------- C5: 客户端可控的 chunk_count 必须有界且自洽 ----------
    std::cout << "\n[C5] UploadFile 的 filesize/chunk_count 校验" << std::endl;
    {
        filestore::UploadFileRequest req;
        req.set_token(tokenA);
        req.set_path("/overflow_" + tag + ".bin");
        req.set_filesize(1000);
        req.set_chunk_count(INT32_MAX);   // 谎报：修复前会按此分配/插入
        filestore::UploadFileResponse resp;
        MprpcController ctl;
        metaStub().UploadFile(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() != 0 &&
                  contains(resp.result().errmsg(), "invalid filesize or chunk_count"),
              "chunk_count=INT32_MAX 被拒", resp.result().errmsg());
    }
    {
        filestore::UploadFileRequest req;
        req.set_token(tokenA);
        req.set_path("/mismatch_" + tag + ".bin");
        req.set_filesize(1000);
        req.set_chunk_count(9);           // 与 filesize 不自洽
        filestore::UploadFileResponse resp;
        MprpcController ctl;
        metaStub().UploadFile(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() != 0 &&
                  contains(resp.result().errmsg(), "invalid filesize or chunk_count"),
              "chunk_count 与 filesize 不自洽被拒", resp.result().errmsg());
    }

    // ---------- S5: 存储端票据校验 ----------
    std::cout << "\n[S5] 存储端票据校验（修复前 6 个 RPC 全部无鉴权）" << std::endl;
    const filestore::ChunkLocation& loc0 = fileA.chunks.at(0);
    {
        filestore::GetChunksBatchRequest breq;
        breq.set_file_id(fileA.fileId);
        filestore::GetChunkSpec* s = breq.add_chunks();
        s->set_chunk_index(0);
        s->set_offset(0);
        s->set_size(16);
        filestore::GetChunksBatchResponse bresp;
        MprpcChannel ch(loc0.ip(), static_cast<uint16_t>(loc0.port()));
        filestore::StorageServiceRpc_Stub stub(&ch);
        MprpcController bctl;
        stub.GetChunksBatch(&bctl, &breq, &bresp, nullptr);   // 票据为空
        check(!bctl.Failed() && bresp.result().errcode() != 0 &&
                  contains(bresp.result().errmsg(), "ticket"),
              "GetChunksBatch 空票据被拒", bresp.result().errmsg());
    }
    {
        filestore::GetChunksBatchRequest breq;
        breq.set_file_id(fileA.fileId);
        breq.set_ticket("deadbeef:1:1:get:0123456789abcdef");   // 伪造
        filestore::GetChunkSpec* s = breq.add_chunks();
        s->set_chunk_index(0);
        s->set_offset(0);
        s->set_size(16);
        filestore::GetChunksBatchResponse bresp;
        MprpcChannel ch(loc0.ip(), static_cast<uint16_t>(loc0.port()));
        filestore::StorageServiceRpc_Stub stub(&ch);
        MprpcController bctl;
        stub.GetChunksBatch(&bctl, &breq, &bresp, nullptr);
        check(!bctl.Failed() && bresp.result().errcode() != 0 &&
                  contains(bresp.result().errmsg(), "ticket"),
              "GetChunksBatch 伪造票据被拒", bresp.result().errmsg());
    }
    {
        // 票据绑定的是 fileA，却拿它去读 fileB 的 file_id —— 即使 fileB 已删，也应先被票据挡下
        filestore::GetChunksBatchRequest breq;
        breq.set_file_id(fileB.fileId);
        breq.set_ticket(fileA.getTicket);
        filestore::GetChunkSpec* s = breq.add_chunks();
        s->set_chunk_index(0);
        s->set_offset(0);
        s->set_size(16);
        filestore::GetChunksBatchResponse bresp;
        MprpcChannel ch(loc0.ip(), static_cast<uint16_t>(loc0.port()));
        filestore::StorageServiceRpc_Stub stub(&ch);
        MprpcController bctl;
        stub.GetChunksBatch(&bctl, &breq, &bresp, nullptr);
        check(!bctl.Failed() && bresp.result().errcode() != 0 &&
                  contains(bresp.result().errmsg(), "ticket"),
              "GetChunksBatch 票据 file_id 不匹配被拒", bresp.result().errmsg());
    }
    {
        // 操作不匹配：put 票据不能用于 get
        filestore::GetChunksBatchRequest breq;
        breq.set_file_id(fileA.fileId);
        breq.set_ticket(fileA.putTicket);
        filestore::GetChunkSpec* s = breq.add_chunks();
        s->set_chunk_index(0);
        s->set_offset(0);
        s->set_size(16);
        filestore::GetChunksBatchResponse bresp;
        MprpcChannel ch(loc0.ip(), static_cast<uint16_t>(loc0.port()));
        filestore::StorageServiceRpc_Stub stub(&ch);
        MprpcController bctl;
        stub.GetChunksBatch(&bctl, &breq, &bresp, nullptr);
        check(!bctl.Failed() && bresp.result().errcode() != 0 &&
                  contains(bresp.result().errmsg(), "ticket"),
              "GetChunksBatch 票据 op 不匹配被拒（put 用于 get）", bresp.result().errmsg());
    }
    {
        // 合法票据应成功——证明上面几条不是「一律拒绝」
        filestore::GetChunksBatchRequest breq;
        breq.set_file_id(fileA.fileId);
        breq.set_ticket(fileA.getTicket);
        filestore::GetChunkSpec* s = breq.add_chunks();
        s->set_chunk_index(0);
        s->set_offset(0);
        s->set_size(16);
        filestore::GetChunksBatchResponse bresp;
        MprpcChannel ch(loc0.ip(), static_cast<uint16_t>(loc0.port()));
        filestore::StorageServiceRpc_Stub stub(&ch);
        MprpcController bctl;
        stub.GetChunksBatch(&bctl, &breq, &bresp, nullptr);
        check(!bctl.Failed() && bresp.result().errcode() == 0 && bresp.chunks_size() == 1,
              "GetChunksBatch 合法票据成功", bresp.result().errmsg());
    }

    // ---------- C5（存储端）: 越界 size 不得让节点崩溃 ----------
    std::cout << "\n[C5] 存储端越界 size 校验（修复前负 size 会抛异常终止进程）" << std::endl;
    {
        filestore::GetChunksBatchRequest breq;
        breq.set_file_id(fileA.fileId);
        breq.set_ticket(fileA.getTicket);
        filestore::GetChunkSpec* s = breq.add_chunks();
        s->set_chunk_index(0);
        s->set_offset(0);
        s->set_size(-1);
        filestore::GetChunksBatchResponse bresp;
        MprpcChannel ch(loc0.ip(), static_cast<uint16_t>(loc0.port()));
        filestore::StorageServiceRpc_Stub stub(&ch);
        MprpcController bctl;
        stub.GetChunksBatch(&bctl, &breq, &bresp, nullptr);
        check(!bctl.Failed() && bresp.result().errcode() != 0 &&
                  contains(bresp.result().errmsg(), "invalid offset or size"),
              "size=-1 被拒", bresp.result().errmsg());
    }
    {
        filestore::GetChunksBatchRequest breq;
        breq.set_file_id(fileA.fileId);
        breq.set_ticket(fileA.getTicket);
        filestore::GetChunkSpec* s = breq.add_chunks();
        s->set_chunk_index(0);
        s->set_offset(0);
        s->set_size(CHUNK_SIZE + 1);
        filestore::GetChunksBatchResponse bresp;
        MprpcChannel ch(loc0.ip(), static_cast<uint16_t>(loc0.port()));
        filestore::StorageServiceRpc_Stub stub(&ch);
        MprpcController bctl;
        stub.GetChunksBatch(&bctl, &breq, &bresp, nullptr);
        check(!bctl.Failed() && bresp.result().errcode() != 0 &&
                  contains(bresp.result().errmsg(), "invalid offset or size"),
              "size>CHUNK_SIZE 被拒", bresp.result().errmsg());
    }
    {
        // 节点仍然存活：再来一次正常请求
        filestore::GetChunksBatchRequest breq;
        breq.set_file_id(fileA.fileId);
        breq.set_ticket(fileA.getTicket);
        filestore::GetChunkSpec* s = breq.add_chunks();
        s->set_chunk_index(0);
        s->set_offset(0);
        s->set_size(16);
        filestore::GetChunksBatchResponse bresp;
        MprpcChannel ch(loc0.ip(), static_cast<uint16_t>(loc0.port()));
        filestore::StorageServiceRpc_Stub stub(&ch);
        MprpcController bctl;
        stub.GetChunksBatch(&bctl, &breq, &bresp, nullptr);
        check(!bctl.Failed() && bresp.result().errcode() == 0,
              "越界请求后存储节点仍存活且可正常服务", bresp.result().errmsg());
    }

    // ---------- S4: AddCleanupTask 的 SSRF 防护 ----------
    std::cout << "\n[S4] AddCleanupTask 鉴权与节点白名单（修复前可指向任意 ip:port）" << std::endl;
    {
        filestore::AddCleanupTaskRequest req;
        req.set_node_ip("1.2.3.4");
        req.set_node_port(9999);
        req.set_file_id(fileA.fileId);
        filestore::AddCleanupTaskResponse resp;
        MprpcController ctl;
        metaStub().AddCleanupTask(&ctl, &req, &resp, nullptr);   // 无 token
        check(!ctl.Failed() && resp.result().errcode() != 0 &&
                  resp.result().errmsg() == "not logged in",
              "无 token 被拒", resp.result().errmsg());
    }
    {
        filestore::AddCleanupTaskRequest req;
        req.set_token(tokenA);
        req.set_node_ip("1.2.3.4");
        req.set_node_port(9999);
        req.set_file_id(fileA.fileId);
        filestore::AddCleanupTaskResponse resp;
        MprpcController ctl;
        metaStub().AddCleanupTask(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() != 0 &&
                  resp.result().errmsg() == "unknown node",
              "合法 token + 环外节点被拒", resp.result().errmsg());
    }
    {
        filestore::AddCleanupTaskRequest req;
        req.set_token(tokenA);
        req.set_node_ip(loc0.ip());
        req.set_node_port(loc0.port() + 1000);   // 环内 IP、错误端口
        req.set_file_id(fileA.fileId);
        filestore::AddCleanupTaskResponse resp;
        MprpcController ctl;
        metaStub().AddCleanupTask(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() != 0 &&
                  resp.result().errmsg() == "unknown node",
              "环内 IP 配错端口同样被拒", resp.result().errmsg());
    }

    // ---------- C1: zhandle 泄漏 ----------
    std::cout << "\n[C1] 服务发现模式下每次 RPC 泄漏 zhandle/线程" << std::endl;
    {
        int before = threadCount();
        for (int i = 0; i < 50; ++i) {
            filestore::ListDirRequest req;
            req.set_token(tokenA);
            req.set_path("/");
            filestore::ListDirResponse resp;
            MprpcController ctl;
            metaStub().ListDir(&ctl, &req, &resp, nullptr);
        }
        int after = threadCount();
        check(before >= 0 && after >= 0 && after <= before + 5,
              "连续 50 次元数据 RPC 后线程数不随调用数增长（before=" +
                  std::to_string(before) + " after=" + std::to_string(after) +
                  "；修复前每次 +1）");
    }

    // ---------- S3: 已 COMPLETE 的文件不允许走取消路径 ----------
    std::cout << "\n[S3] CancelUpload 状态机（修复前会删空已提交文件的块记录）" << std::endl;
    {
        filestore::CancelUploadRequest req;
        req.set_token(tokenA);
        req.set_file_id(fileA.fileId);   // fileA 已 CommitUpload，处于 COMPLETE
        filestore::CancelUploadResponse resp;
        MprpcController ctl;
        metaStub().CancelUpload(&ctl, &req, &resp, nullptr);
        check(!ctl.Failed() && resp.result().errcode() != 0 &&
                  resp.result().errmsg() == "file already committed",
              "对已 COMPLETE 文件调 CancelUpload 被拒", resp.result().errmsg());
    }
    {
        filestore::QueryFileResponse q;
        std::string qerr;
        bool ok = queryFile(tokenA, pathA, q, qerr);
        check(ok && q.chunk_count() == fileA.chunkCount,
              "被拒绝的取消未破坏块记录（文件仍完整）", qerr);
    }

    // ---------- 收尾：清理本程序创建的文件 ----------
    std::cout << "\n[清理] 删除本程序创建的文件" << std::endl;
    {
        filestore::GetFileNodesResponse g;
        std::string gerr;
        if (getFileNodes(tokenA, pathA, g, gerr)) {
            filestore::DeleteFileRequest req;
            req.set_token(tokenA);
            req.set_file_id(g.file_id());
            filestore::DeleteFileResponse resp;
            MprpcController ctl;
            metaStub().DeleteFile(&ctl, &req, &resp, nullptr);
            for (int i = 0; i < g.nodes_size(); ++i) {
                filestore::DeleteFileRequest dreq;
                dreq.set_file_id(g.file_id());
                dreq.set_ticket(g.ticket());
                filestore::DeleteFileResponse dresp;
                MprpcChannel ch(g.nodes(i).ip(), static_cast<uint16_t>(g.nodes(i).port()));
                filestore::StorageServiceRpc_Stub stub(&ch);
                MprpcController dctl;
                stub.DeleteFile(&dctl, &dreq, &dresp, nullptr);
            }
            check(resp.result().errcode() == 0, "清理 A 的文件", resp.result().errmsg());
        }
    }

    std::cout << "\n===== 结果：通过 " << g_pass << " / 失败 " << g_fail << " =====" << std::endl;
    return g_fail == 0 ? 0 : 1;
}
