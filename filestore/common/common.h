#pragma once

#include <climits>
#include <string>
#include <openssl/md5.h>
#include <openssl/sha.h>

// 文件分块大小（存储服务端与客户端共享，避免两处定义漂移）
constexpr int CHUNK_SIZE = 4 * 1024 * 1024;

// 宽松解析非负十进制整数：含非数字字符、超长、或超出 int 范围均返回 false。
// 用于处理**不可信来源**的数字（数据目录里的文件名、Redis 会话值等）——
// std::stoi 在这些输入上会抛 std::invalid_argument / std::out_of_range，
// 而调用方常在 detached 线程里，异常逃逸会直接 terminate 掉整个进程。
// 注意仅靠「位数 ≤ 10」不足以拦住溢出：7000000847 是 10 位但已超过 INT_MAX。
inline bool parseNonNegativeInt(const std::string& s, int& out)
{
    if (s.empty() || s.size() > 10) {
        return false;
    }
    for (char c : s) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    // 10 位十进制最大 9999999999，用 long long 承接不会溢出，随后显式判 int 范围
    long long v = std::stoll(s);
    if (v > INT_MAX) {
        return false;
    }
    out = static_cast<int>(v);
    return true;
}

// 计算数据的 MD5，返回 32 字符小写十六进制
static inline std::string md5Hex(const std::string& data)
{
    unsigned char digest[MD5_DIGEST_LENGTH];
    MD5(reinterpret_cast<const unsigned char*>(data.data()), data.size(), digest);

    static const char* hex = "0123456789abcdef";
    std::string result;
    result.reserve(MD5_DIGEST_LENGTH * 2);
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        result.push_back(hex[digest[i] >> 4]);
        result.push_back(hex[digest[i] & 0x0F]);
    }
    return result;
}

// 计算数据的 SHA256，返回 64 字符小写十六进制（密码散列用）
static inline std::string sha256Hex(const std::string& data)
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(), digest);

    static const char* hex = "0123456789abcdef";
    std::string result;
    result.reserve(SHA256_DIGEST_LENGTH * 2);
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        result.push_back(hex[digest[i] >> 4]);
        result.push_back(hex[digest[i] & 0x0F]);
    }
    return result;
}
