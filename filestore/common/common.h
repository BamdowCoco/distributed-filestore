#pragma once

#include <climits>
#include <cstdint>
#include <string>
#include <openssl/md5.h>
#include <openssl/sha.h>

// 文件分块大小（存储服务端与客户端共享，避免两处定义漂移）
constexpr int CHUNK_SIZE = 4 * 1024 * 1024;

// 宽松解析非负十进制整数（int64 版）：含非数字字符或超长均返回 false。
// 实现用**逐位累加 + 位数上限**，既不抛异常也不会在累加过程中溢出
// （stdlib 的 std::stoi/std::stoll 对畸形/超长输入会抛，而调用方常在 detached 线程里，
// 异常逃逸会直接 terminate 掉整个进程——本项目已因此踩过两次）。
inline bool parseNonNegativeInt64(const std::string& s, int64_t& out)
{
    if (s.empty() || s.size() > 18) {
        return false;   // 18 位以内，累加不会溢出 int64
    }
    int64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') {
            return false;
        }
        v = v * 10 + (c - '0');
    }
    out = v;
    return true;
}

// int 版：在 int64 版基础上额外要求不超过 INT_MAX。
// 注意仅靠「位数 ≤ 10」不足以拦住溢出：7000000847 是 10 位但已超过 INT_MAX。
inline bool parseNonNegativeInt(const std::string& s, int& out)
{
    int64_t v = 0;
    if (!parseNonNegativeInt64(s, v) || v > INT_MAX) {
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
