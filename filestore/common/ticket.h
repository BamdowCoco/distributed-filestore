#pragma once

#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <string>
#include <vector>

#include <openssl/crypto.h>
#include <openssl/hmac.h>

// 存储访问票据：绑定 (userId, fileId, op, 过期时间) 的 HMAC-SHA256。
//
// 元数据服务用共享密钥签发，存储节点用同一密钥离线验签。相比「存储节点拿 token
// 去 Redis 查会话」，票据方案有两个关键优势：
//   1. 真正完成授权——token 只能证明调用者是某个已登录用户，而 file_id 是全局自增的，
//      无法阻止用户 A 用 B 的 file_id 读写；票据把 file_id 与 op 一并签名绑定，
//      存储节点无需知道归属表即可确认这次访问在授权范围内；
//   2. 存储节点不依赖 Redis，少一个数据面单点。
//
// 编码格式（冒号分隔，共 5 段）：
//   <expiryUnix>:<userId>:<fileId>:<op>:<hmacHex>
// HMAC 覆盖前四段拼接出的字符串。
//
// 授权语义由 (fileId, op, 有效期) + 签名共同承载；userId 段仅用于审计与日志
// （非文件维度的操作如 ListFiles 用 fileId=0、userId=0）。

// 解析票据共享密钥：环境变量 MPRPC_TICKET_SECRET 优先，其次取配置文件里的值。
// 真实密钥不应写进受版本控制的配置文件，故环境变量是推荐途径；
// 两者都缺失时返回空串，调用方必须拒绝启动（fail closed），不要退回默认值。
inline std::string resolveTicketSecret(const std::string& fromConfig)
{
    const char* env = std::getenv("MPRPC_TICKET_SECRET");
    if (env != nullptr && *env != '\0') {
        return env;
    }
    return fromConfig;
}

// 计算 HMAC-SHA256，返回 64 字符小写十六进制
inline std::string hmacSha256Hex(const std::string& key, const std::string& data)
{
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int len = 0;
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(), digest, &len);

    static const char* hex = "0123456789abcdef";
    std::string result;
    result.reserve(len * 2);
    for (unsigned int i = 0; i < len; ++i) {
        result.push_back(hex[digest[i] >> 4]);
        result.push_back(hex[digest[i] & 0x0F]);
    }
    return result;
}

// 宽松解析非负十进制整数，非法返回 false（避免 stoi/stoll 抛异常）
inline bool ticketParseInt(const std::string& s, int64_t& out)
{
    if (s.empty() || s.size() > 18) {
        return false;
    }
    for (char c : s) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    out = std::stoll(s);
    return true;
}

// 签发票据；ttlSec 为有效期（秒）
inline std::string makeTicket(const std::string& secret, int userId, int32_t fileId,
                              const std::string& op, int ttlSec)
{
    int64_t expiry = static_cast<int64_t>(std::time(nullptr)) + ttlSec;
    std::string payload = std::to_string(expiry) + ":" + std::to_string(userId) + ":" +
                          std::to_string(fileId) + ":" + op;
    return payload + ":" + hmacSha256Hex(secret, payload);
}

// 验签：票据有效且 fileId/op 均匹配时返回 true 并输出 userId，否则返回 false
inline bool verifyTicket(const std::string& secret, const std::string& ticket,
                         int32_t fileId, const std::string& op, int& userId)
{
    if (secret.empty() || ticket.empty()) {
        return false;
    }

    std::vector<std::string> parts;
    size_t start = 0;
    while (true) {
        size_t pos = ticket.find(':', start);
        if (pos == std::string::npos) {
            parts.push_back(ticket.substr(start));
            break;
        }
        parts.push_back(ticket.substr(start, pos - start));
        start = pos + 1;
    }
    if (parts.size() != 5) {
        return false;
    }

    // 操作的绑定最便宜，先查
    if (parts[3] != op) {
        return false;
    }

    int64_t expiry = 0;
    int64_t ticketFileId = 0;
    int64_t ticketUserId = 0;
    if (!ticketParseInt(parts[0], expiry) ||
        !ticketParseInt(parts[1], ticketUserId) ||
        !ticketParseInt(parts[2], ticketFileId)) {
        return false;
    }
    if (ticketFileId != fileId) {
        return false;
    }
    if (expiry < static_cast<int64_t>(std::time(nullptr))) {
        return false;   // 已过期
    }

    std::string payload = parts[0] + ":" + parts[1] + ":" + parts[2] + ":" + parts[3];
    std::string expect = hmacSha256Hex(secret, payload);
    // 定长比较，避免按字节提前返回泄露签名信息
    if (expect.size() != parts[4].size() ||
        CRYPTO_memcmp(expect.data(), parts[4].data(), expect.size()) != 0) {
        return false;
    }

    userId = static_cast<int>(ticketUserId);
    return true;
}
