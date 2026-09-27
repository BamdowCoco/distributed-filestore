#pragma once

#include <string>
#include <vector>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include "common.h"   // parseNonNegativeInt、sha256Hex（校验历史遗留格式用）

// 口令散列：PBKDF2-HMAC-SHA256 + 每用户随机盐。
//
// 存储格式：pbkdf2-sha256$<iterations>$<base64 salt>$<base64 hash>
// 不用 crypt(3)/openssl passwd 之类：不引额外依赖，且格式自解释——日后换算法只需
// 改前缀并保留一条旧分支。base64 字母表（含 '=' 填充）不含 '$'，故用它作分隔符安全。
//
// **兼容历史遗留的无盐 SHA256**：早期版本直接存 sha256Hex(password)（64 位十六进制，
// 无盐、可离线爆破）。verifyPassword 按格式分派，所以既有账号仍能登录；
// 返回 OkNeedsRehash 时调用方应顺手把存储值升级为新格式（无需停机、无需批量作业）。

namespace password {

// PBKDF2-HMAC-SHA256 迭代次数。调高更安全但登录/注册更慢（本机约几十毫秒量级）。
constexpr int kIterations = 100000;
constexpr int kSaltBytes = 16;
constexpr int kHashBytes = 32;
// 迭代次数的合理上界：防御存储值被篡改成极大的迭代数导致登录线程被拖死
constexpr int kMaxAcceptableIterations = 10000000;

inline std::string base64Encode(const unsigned char* data, int len)
{
    if (data == nullptr || len <= 0) {
        return std::string();
    }
    std::string out(static_cast<size_t>(4 * ((len + 2) / 3)), '\0');
    int n = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(&out[0]), data, len);
    if (n <= 0) {
        return std::string();
    }
    out.resize(static_cast<size_t>(n));
    return out;
}

inline bool base64Decode(const std::string& in, std::vector<unsigned char>& out)
{
    if (in.empty() || in.size() % 4 != 0) {
        return false;
    }
    out.assign(in.size() / 4 * 3, 0);
    int n = EVP_DecodeBlock(out.data(), reinterpret_cast<const unsigned char*>(in.data()),
                            static_cast<int>(in.size()));
    if (n < 0) {
        return false;
    }
    // EVP_DecodeBlock 的返回值**包含**填充字节，需要按 '=' 的个数扣掉
    size_t pad = 0;
    if (in[in.size() - 1] == '=') {
        ++pad;
    }
    if (in.size() >= 2 && in[in.size() - 2] == '=') {
        ++pad;
    }
    out.resize(static_cast<size_t>(n) - pad);
    return true;
}

// 生成新格式散列。返回空串表示失败（取随机数/派生失败）——调用方必须当成
// 注册失败处理，**不得**降级去存明文或旧格式。
inline std::string hashPassword(const std::string& password)
{
    unsigned char salt[kSaltBytes];
    if (RAND_bytes(salt, kSaltBytes) != 1) {
        return std::string();
    }
    unsigned char hash[kHashBytes];
    if (PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()),
                          salt, kSaltBytes, kIterations, EVP_sha256(),
                          kHashBytes, hash) != 1) {
        return std::string();
    }
    return "pbkdf2-sha256$" + std::to_string(kIterations) + "$" +
           base64Encode(salt, kSaltBytes) + "$" + base64Encode(hash, kHashBytes);
}

enum class VerifyResult
{
    Ok,             // 匹配，且已是当前参数的新格式
    OkNeedsRehash,  // 匹配，但存储值是旧格式或旧参数——调用方应升级
    Mismatch        // 不匹配（含存储值格式非法，一律按失败处理，fail closed）
};

inline bool isLowerHex(const std::string& s)
{
    for (char c : s) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            return false;
        }
    }
    return !s.empty();
}

inline VerifyResult verifyPassword(const std::string& password, const std::string& stored)
{
    const std::string prefix = "pbkdf2-sha256$";

    if (stored.compare(0, prefix.size(), prefix) == 0) {
        // 切成 4 段：算法、迭代数、盐、散列
        std::vector<std::string> parts;
        size_t start = 0;
        while (true) {
            size_t pos = stored.find('$', start);
            if (pos == std::string::npos) {
                parts.push_back(stored.substr(start));
                break;
            }
            parts.push_back(stored.substr(start, pos - start));
            start = pos + 1;
        }
        if (parts.size() != 4) {
            return VerifyResult::Mismatch;
        }
        int iterations = 0;
        if (!parseNonNegativeInt(parts[1], iterations) ||
            iterations <= 0 || iterations > kMaxAcceptableIterations) {
            return VerifyResult::Mismatch;
        }
        std::vector<unsigned char> salt;
        std::vector<unsigned char> expect;
        if (!base64Decode(parts[2], salt) || !base64Decode(parts[3], expect) ||
            salt.empty() || expect.empty()) {
            return VerifyResult::Mismatch;
        }

        std::vector<unsigned char> got(expect.size(), 0);
        if (PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()),
                              salt.data(), static_cast<int>(salt.size()),
                              iterations, EVP_sha256(),
                              static_cast<int>(got.size()), got.data()) != 1) {
            return VerifyResult::Mismatch;
        }
        // 定长比较，避免按字节提前返回泄露信息
        if (CRYPTO_memcmp(got.data(), expect.data(), got.size()) != 0) {
            return VerifyResult::Mismatch;
        }
        // 参数也一并升级：迭代数偏低时同样值得重算
        return (iterations == kIterations) ? VerifyResult::Ok : VerifyResult::OkNeedsRehash;
    }

    // 历史遗留：无盐 SHA256 的 64 位小写十六进制
    if (stored.size() == 64 && isLowerHex(stored)) {
        std::string got = sha256Hex(password);
        if (got.size() == stored.size() &&
            CRYPTO_memcmp(got.data(), stored.data(), got.size()) == 0) {
            return VerifyResult::OkNeedsRehash;
        }
    }

    return VerifyResult::Mismatch;
}

}  // namespace password
