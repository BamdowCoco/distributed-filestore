#pragma once

#include <cstdint>
#include <set>
#include <string>

#include "file_storage.pb.h"
#include "mprpc_channel.h"

// 客户端：封装鉴权、虚拟文件树操作与文件上传/下载/删除。
// 登录 token 与当前目录存本地（~/.fscli/），由本类内部读写。
class FsClient
{
public:
    FsClient();

    // ---- 鉴权 ----
    // 注册：调用 meta Register 创建账号
    bool registerUser(const std::string& username, const std::string& password);
    // 登录：调用 meta Login，成功后本地保存 token
    bool login(const std::string& username, const std::string& password);
    // 登出：调用 meta Logout 并清除本地 token
    bool logout();

    // ---- 目录 ----
    // 建目录：调用 meta Mkdir
    bool mkdir(const std::string& path);
    // 删目录：调用 meta Rmdir（recursive 控制是否递归）
    bool rmdir(const std::string& path, bool recursive = false);
    // 列目录：调用 meta ListDir 并打印条目
    bool listDir(const std::string& path);

    // cd：切换当前目录（校验目标目录存在后保存本地 cwd）
    bool changeDir(const std::string& path);

    // ---- 文件 ----
    // 上传：登记 -> 按节点分组子批上传 -> 提交
    bool upload(const std::string& localFile, const std::string& virtualPath);
    // 下载：查块位置 -> 分组批量下载 -> 校验和回填
    bool download(const std::string& virtualPath, const std::string& dest = "");
    // 删除：查节点 -> 删元数据 -> 逐节点删数据
    bool remove(const std::string& virtualPath);

private:
    // 相对路径 -> 绝对虚拟路径（用本地 cwd）
    static std::string resolvePath(const std::string& input);

    // 批量 RPC 辅助（同节点复用会话连接）
    bool putChunksBatch(const std::string& ip, uint16_t port, MprpcChannel& channel,
                        const filestore::PutChunksBatchRequest& breq,
                        filestore::PutChunksBatchResponse& bresp);
    bool getChunksBatch(const std::string& ip, uint16_t port, MprpcChannel& channel,
                        const filestore::GetChunksBatchRequest& breq,
                        filestore::GetChunksBatchResponse& bresp);
    bool deleteFile(const std::string& ip, uint16_t port, int32_t fileId);

    // 上传失败回滚：删已传块 + 取消元数据 PENDING 记录
    void rollbackUpload(int32_t fileId, const std::set<std::string>& nodes);

    MprpcChannel m_metaChannel;
    filestore::MetaServiceRpc_Stub m_metaStub;
};
