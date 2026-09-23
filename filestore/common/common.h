#pragma once

#include <string>
#include <openssl/md5.h>

// 文件分块大小（存储服务端与客户端共享，避免两处定义漂移）
constexpr int CHUNK_SIZE = 4 * 1024 * 1024;

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
