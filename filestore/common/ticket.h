#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <vector>

#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#include "common.h"   // sha256Hex（算 kid 用）

// 存储访问票据：Ed25519 非对称签名。
//
// 为什么从 HMAC 换成非对称：HMAC 用的是**共享密钥**，必须分发到每一台存储节点，
// 且一台存储节点被攻破就等于拿到签发能力——攻击者可为任意 (file_id, op) 伪造票据，
// 等于变成第二个不受控的元数据服务。换成非对称后，元数据只持私钥、存储节点只持公钥，
// 被拿下的存储节点能验不能签。顺带把「密钥怎么安全分发到多台机器」这个问题也解掉了：
// **公钥不是机密**，写进各节点配置文件本来就安全（唯一的机密只在一台机器上）。
//
// 票据格式（v2）：
//   v2:<kid>:<expiry>:<userId>:<fileId>:<op>:<sigHex>
// - kid   = SHA256(SPKI DER) 的前 32 个十六进制字符（128 bit）。整个 kid 一律由
//           i2d_PUBKEY 的**规范编码**算出，PEM/DER 等不同来源因此归一到同一条路径，
//           否则会出现「同一把密钥两边算出不同 kid」的静默脑裂；
// - sig   = 对**末段之前整个字面字符串**（含版本与 kid）做 Ed25519 签名。
//           必须是字面前缀而不是「解析成整数再重新序列化」，否则会引入
//           前导零之类的规范化分歧；
// - sigHex = 128 个十六进制字符（Ed25519 签名固定 64 字节）。选 hex 而非 base64：
//           无需解码、与既有风格一致，且 ':' 不在 hex 字母表内，冒号分隔天然安全。
//
// 不做 HMAC(v1) 双模式并存：本项目没有已部署的机群，留一条更弱的路径没有收益。
// 这是**停机式切换**；真实滚动升级需要一段同时接受两种票据的过渡期。

namespace ticket {

// version 前缀，验签时严格比对
constexpr const char* kTicketPrefix = "v2";
// kid / 签名的十六进制长度
constexpr size_t kKidHexLen = 32;
constexpr size_t kSigHexLen = 128;
// 过期判断的时钟偏差容忍（秒）：元数据与存储节点的时钟不可能完全一致
constexpr int64_t kClockSkewSec = 60;

// 一把 Ed25519 密钥（私钥或公钥）。RAII 持有 EVP_PKEY，禁止拷贝。
class TicketKey
{
public:
    TicketKey() = default;
    ~TicketKey() { reset(); }

    TicketKey(const TicketKey&) = delete;
    TicketKey& operator=(const TicketKey&) = delete;

    TicketKey(TicketKey&& other) noexcept : m_key(other.m_key), m_kid(std::move(other.m_kid))
    {
        other.m_key = nullptr;
    }
    TicketKey& operator=(TicketKey&& other) noexcept
    {
        if (this != &other) {
            reset();
            m_key = other.m_key;
            m_kid = std::move(other.m_kid);
            other.m_key = nullptr;
        }
        return *this;
    }

    // 从 PEM 文件加载**私钥**（签发用）
    bool loadPrivatePem(const std::string& path)
    {
        return loadPem(path, true);
    }

    // 从 PEM 文件加载**公钥**（验签用）
    bool loadPublicPem(const std::string& path)
    {
        return loadPem(path, false);
    }

    // 接管一把已有的 EVP_PKEY（就地生成的密钥、或测试用）。校验类型后取得所有权，
    // 失败时不接管、返回 false。
    bool adopt(EVP_PKEY* key)
    {
        reset();
        if (key == nullptr) {
            return false;
        }
        if (EVP_PKEY_id(key) != EVP_PKEY_ED25519) {
            EVP_PKEY_free(key);
            return false;
        }
        m_key = key;
        return true;
    }

    bool valid() const { return m_key != nullptr; }

    // 密钥标识：SHA256(SPKI DER) 的前 32 个十六进制字符。首次调用时计算并缓存。
    std::string kid() const
    {
        if (!m_key || !m_kid.empty()) {
            return m_kid;
        }
        unsigned char* der = nullptr;
        int derLen = i2d_PUBKEY(m_key, &der);
        if (derLen <= 0 || der == nullptr) {
            return std::string();
        }
        m_kid = sha256Hex(std::string(reinterpret_cast<char*>(der), static_cast<size_t>(derLen)))
                    .substr(0, kKidHexLen);
        OPENSSL_free(der);
        return m_kid;
    }

    EVP_PKEY* raw() const { return m_key; }

private:
    void reset()
    {
        if (m_key != nullptr) {
            EVP_PKEY_free(m_key);
            m_key = nullptr;
        }
        m_kid.clear();
    }

    bool loadPem(const std::string& path, bool wantPrivate)
    {
        reset();
        if (path.empty()) {
            return false;
        }
        FILE* fp = fopen(path.c_str(), "r");
        if (fp == nullptr) {
            return false;
        }
        EVP_PKEY* key = wantPrivate ? PEM_read_PrivateKey(fp, nullptr, nullptr, nullptr)
                                    : PEM_read_PUBKEY(fp, nullptr, nullptr, nullptr);
        fclose(fp);
        if (key == nullptr) {
            // 可能根本不是 PEM，或口令保护的私钥（我们只支持未加密 PKCS#8）
            ERR_clear_error();
            return false;
        }
        // 必须确认就是 Ed25519：否则一把 RSA 密钥也会「加载成功」，
        // 直到第一次签名才失败，错误点离原因太远
        if (EVP_PKEY_id(key) != EVP_PKEY_ED25519) {
            EVP_PKEY_free(key);
            return false;
        }
        m_key = key;
        return true;
    }

    EVP_PKEY* m_key = nullptr;
    mutable std::string m_kid;
};

// 宽松解析非负十进制整数（票据里的 expiry/userId/fileId）
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

inline bool isLowerHex(const std::string& s, size_t expectedLen)
{
    if (s.size() != expectedLen) {
        return false;
    }
    for (char c : s) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            return false;
        }
    }
    return true;
}

inline std::string toHex(const unsigned char* data, size_t len)
{
    static const char* hex = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (size_t i = 0; i < len; ++i) {
        out.push_back(hex[data[i] >> 4]);
        out.push_back(hex[data[i] & 0x0F]);
    }
    return out;
}

inline bool fromHex(const std::string& in, std::vector<unsigned char>& out)
{
    if (in.size() % 2 != 0) {
        return false;
    }
    out.clear();
    out.reserve(in.size() / 2);
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        return -1;
    };
    for (size_t i = 0; i < in.size(); i += 2) {
        int hi = nibble(in[i]);
        int lo = nibble(in[i + 1]);
        if (hi < 0 || lo < 0) {
            return false;
        }
        out.push_back(static_cast<unsigned char>((hi << 4) | lo));
    }
    return true;
}

// 用私钥签发票据。失败返回空串（调用方必须当作失败，不得降级）。
inline std::string makeTicket(EVP_PKEY* privateKey, int userId, int32_t fileId,
                              const std::string& op, int ttlSec)
{
    if (privateKey == nullptr || op.empty()) {
        return std::string();
    }
    unsigned char* der = nullptr;
    int derLen = i2d_PUBKEY(privateKey, &der);
    if (derLen <= 0 || der == nullptr) {
        return std::string();
    }
    std::string kid = sha256Hex(std::string(reinterpret_cast<char*>(der),
                                            static_cast<size_t>(derLen)))
                          .substr(0, kKidHexLen);
    OPENSSL_free(der);

    int64_t expiry = static_cast<int64_t>(std::time(nullptr)) + ttlSec;
    std::string payload = std::string(kTicketPrefix) + ":" + kid + ":" +
                          std::to_string(expiry) + ":" + std::to_string(userId) + ":" +
                          std::to_string(fileId) + ":" + op;

    // Ed25519 只能一次性签验：1.1.1 不支持 DigestSignUpdate/Final，md 必须为 NULL
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (ctx == nullptr) {
        return std::string();
    }
    size_t sigLen = 0;
    bool ok = EVP_DigestSignInit(ctx, nullptr, nullptr, nullptr, privateKey) == 1 &&
              EVP_DigestSign(ctx, nullptr, &sigLen,
                             reinterpret_cast<const unsigned char*>(payload.data()),
                             payload.size()) == 1;
    std::vector<unsigned char> sig(sigLen);
    if (ok && sigLen > 0) {
        ok = EVP_DigestSign(ctx, sig.data(), &sigLen,
                            reinterpret_cast<const unsigned char*>(payload.data()),
                            payload.size()) == 1;
    }
    EVP_MD_CTX_free(ctx);
    if (!ok || sigLen == 0) {
        return std::string();
    }

    return payload + ":" + toHex(sig.data(), sigLen);
}

// 用可信公钥集合验签票据：kid 必须命中集合中的某一把，且 fileId/op 与调用方一致、未过期。
// 成功时输出 userId。未知 kid 一律拒绝（不能宽容接受，否则是 fail-open）。
inline bool verifyTicket(const std::vector<TicketKey>& trustedKeys, const std::string& ticket,
                         int32_t fileId, const std::string& op, int& userId)
{
    if (ticket.empty() || op.empty() || trustedKeys.empty()) {
        return false;
    }

    // 切成 7 段：v2 / kid / expiry / userId / fileId / op / sig
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
    if (parts.size() != 7) {
        return false;
    }
    if (parts[0] != kTicketPrefix) {
        return false;
    }
    // 先用便宜的检查挡掉畸形输入，再做任何解析
    if (!isLowerHex(parts[1], kKidHexLen) || !isLowerHex(parts[6], kSigHexLen)) {
        return false;
    }
    if (parts[5] != op) {
        return false;
    }

    int64_t expiry = 0;
    int64_t ticketUserId = 0;
    int64_t ticketFileId = 0;
    if (!ticketParseInt(parts[2], expiry) || !ticketParseInt(parts[3], ticketUserId) ||
        !ticketParseInt(parts[4], ticketFileId)) {
        return false;
    }
    if (ticketFileId != fileId || ticketUserId <= 0) {
        return false;
    }
    // 留出时钟偏差容忍：元数据与存储节点的时钟不会完全一致
    if (expiry + kClockSkewSec < static_cast<int64_t>(std::time(nullptr))) {
        return false;
    }

    const TicketKey* matched = nullptr;
    for (const auto& key : trustedKeys) {
        if (key.valid() && key.kid() == parts[1]) {
            matched = &key;
            break;
        }
    }
    if (matched == nullptr) {
        return false;
    }

    // 签名覆盖最后一个冒号之前的全部内容——直接用收到的字面前缀，
    // 不重新序列化解析结果（否则会引入前导零之类的规范化分歧）
    std::string signaturePayload = ticket.substr(0, ticket.size() - parts[6].size() - 1);
    std::vector<unsigned char> sig;
    if (!fromHex(parts[6], sig) || sig.empty()) {
        return false;
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (ctx == nullptr) {
        return false;
    }
    int rc = EVP_DigestVerifyInit(ctx, nullptr, nullptr, nullptr, matched->raw());
    if (rc == 1) {
        rc = EVP_DigestVerify(ctx, sig.data(), sig.size(),
                              reinterpret_cast<const unsigned char*>(signaturePayload.data()),
                              signaturePayload.size());
    }
    EVP_MD_CTX_free(ctx);
    // 只有 1 表示有效；0 是签名不匹配，<0 是其它错误——都不能算通过
    // （签名比较由 OpenSSL 内部定长完成，不需要自己写类似 CRYPTO_memcmp 的比较）
    if (rc != 1) {
        return false;
    }

    userId = static_cast<int>(ticketUserId);
    return true;
}

}  // namespace ticket
