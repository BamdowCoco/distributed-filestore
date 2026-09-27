// 存储访问票据（filestore/common/ticket.h，Ed25519）与不可信数字解析的单元测试。
// 不依赖 ZooKeeper / MySQL / 服务进程。
// 编译：g++ -O2 -std=c++11 test_ticket.cc -o test_ticket -lcrypto
#include <climits>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include <openssl/evp.h>
#include <openssl/pem.h>

#include "../../filestore/common/common.h"
#include "../../filestore/common/ticket.h"

static int g_failed = 0;

static void expect(bool cond, const std::string& what)
{
    std::printf("  %s %s\n", cond ? "ok  " : "FAIL", what.c_str());
    if (!cond) {
        ++g_failed;
    }
}

// 就地生成一把 Ed25519 密钥（私钥与公钥共用同一个 EVP_PKEY）
static EVP_PKEY* genEd25519()
{
    EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, nullptr);
    if (ctx == nullptr) {
        return nullptr;
    }
    EVP_PKEY* key = nullptr;
    if (EVP_PKEY_keygen_init(ctx) != 1 || EVP_PKEY_keygen(ctx, &key) != 1) {
        key = nullptr;
    }
    EVP_PKEY_CTX_free(ctx);
    return key;
}

static bool writePubPem(EVP_PKEY* key, const std::string& path)
{
    FILE* fp = fopen(path.c_str(), "w");
    if (fp == nullptr) {
        return false;
    }
    bool ok = PEM_write_PUBKEY(fp, key) == 1;
    fclose(fp);
    return ok;
}

static bool writePrivPem(EVP_PKEY* key, const std::string& path)
{
    FILE* fp = fopen(path.c_str(), "w");
    if (fp == nullptr) {
        return false;
    }
    bool ok = PEM_write_PrivateKey(fp, key, nullptr, nullptr, 0, nullptr, nullptr) == 1;
    fclose(fp);
    return ok;
}

// 构造一个只含指定公钥 PEM 的可信集合
static std::vector<ticket::TicketKey> trustSet(const std::vector<std::string>& pubPaths)
{
    std::vector<ticket::TicketKey> keys;
    for (const auto& p : pubPaths) {
        ticket::TicketKey k;
        k.loadPublicPem(p);
        keys.push_back(std::move(k));
    }
    return keys;
}

int main()
{
    std::printf("===== 票据（Ed25519）单元测试 =====\n");

    const std::string priv1 = "/tmp/ticket_test_priv1.pem";
    const std::string pub1 = "/tmp/ticket_test_pub1.pem";
    const std::string pub2 = "/tmp/ticket_test_pub2.pem";
    const std::string rsaPub = "/tmp/ticket_test_rsa.pem";

    // ---- 密钥准备 ----
    ticket::TicketKey key1;
    ticket::TicketKey key2;
    if (!key1.adopt(genEd25519()) || !key2.adopt(genEd25519())) {
        std::printf("FATAL 无法生成 Ed25519 密钥\n");
        return 1;
    }
    expect(key1.valid() && key2.valid(), "两把 Ed25519 密钥生成成功");

    const std::string kid1 = key1.kid();
    expect(kid1.size() == ticket::kKidHexLen, "kid 长度为 32 个十六进制字符");
    expect(kid1 == key1.kid(), "同一把密钥算出的 kid 稳定");
    expect(kid1 != key2.kid(), "不同密钥的 kid 不同");

    expect(writePrivPem(key1.raw(), priv1) && writePubPem(key1.raw(), pub1) &&
               writePubPem(key2.raw(), pub2),
           "写出测试用 PEM（私钥 1 / 公钥 1 / 公钥 2）");

    ticket::TicketKey privKey;
    expect(privKey.loadPrivatePem(priv1), "从 PEM 加载私钥");
    expect(privKey.kid() == kid1, "从 PEM 加载后 kid 与直接计算一致");
    {
        ticket::TicketKey pub;
        expect(pub.loadPublicPem(pub1), "从 PEM 加载公钥");
        // kid 由 SPKI 的规范编码算出，因此与「从私钥算还是从公钥算」无关
        expect(pub.kid() == kid1, "公钥侧与私钥侧算出的 kid 一致（规范编码不变量）");
    }

    // ---- 签发与验签 ----
    const int userId = 42;
    const int32_t fileId = 1001;
    std::string good = ticket::makeTicket(privKey.raw(), userId, fileId, "put", 60);
    expect(!good.empty(), "签发成功");
    expect(good.compare(0, 2, "v2") == 0, "票据带 v2 版本前缀");

    std::vector<ticket::TicketKey> trust = trustSet({pub1});
    {
        int out = 0;
        expect(ticket::verifyTicket(trust, good, fileId, "put", out), "合法票据通过验签");
        expect(out == userId, "验签返回签发时的 userId");
    }

    // ---- 各类拒绝 ----
    {
        int out = 0;
        expect(!ticket::verifyTicket(trust, good, fileId, "get", out), "拒绝跨操作使用（get）");
        expect(!ticket::verifyTicket(trust, good, fileId, "del", out), "拒绝跨操作使用（del）");
        expect(!ticket::verifyTicket(trust, good, fileId + 1, "put", out), "拒绝跨 file_id 使用");
    }
    {
        int out = 0;
        std::string expired = ticket::makeTicket(privKey.raw(), userId, fileId, "put",
                                                 -10 * ticket::kClockSkewSec);
        expect(!ticket::verifyTicket(trust, expired, fileId, "put", out),
               "过期票据被拒（超出时钟偏差容忍）");
        // 刚过期但在容忍窗口内：应仍然接受（时钟偏差容忍生效）
        std::string justExpired = ticket::makeTicket(privKey.raw(), userId, fileId, "put", -1);
        expect(ticket::verifyTicket(trust, justExpired, fileId, "put", out),
               "刚过期但在偏差容忍窗口内的票据仍被接受");
    }
    {
        int out = 0;
        std::string tampered = good;
        tampered[tampered.size() - 1] = (tampered[tampered.size() - 1] == 'a') ? 'b' : 'a';
        expect(!ticket::verifyTicket(trust, tampered, fileId, "put", out), "篡改签名后被拒");

        // 只改载荷（file_id）而保留签名：签名覆盖全部字面前缀，改动即失效
        std::string payloadTampered = ticket::makeTicket(privKey.raw(), userId, fileId + 7, "put", 60);
        expect(!ticket::verifyTicket(trust, payloadTampered, fileId, "put", out),
               "签名绑定 file_id，改号即失效");
    }

    // ---- 未知 kid：另一把私钥签发的票据必须被拒（不能宽容接受）----
    {
        std::string foreign = ticket::makeTicket(key2.raw(), userId, fileId, "put", 60);
        int out = 0;
        expect(!ticket::verifyTicket(trust, foreign, fileId, "put", out),
               "未知 kid（另一把私钥签发）被拒");
    }

    // ---- 轮换：先发布新公钥 → 新旧都能验；再退役旧公钥 → 旧的失效 ----
    {
        std::string byKey1 = ticket::makeTicket(privKey.raw(), userId, fileId, "put", 60);
        std::string byKey2 = ticket::makeTicket(key2.raw(), userId, fileId, "put", 60);
        int out = 0;

        // 只信任 key2（新公钥已发布但旧公钥还没退役）
        std::vector<ticket::TicketKey> both = trustSet({pub1, pub2});
        expect(ticket::verifyTicket(both, byKey1, fileId, "put", out),
               "过渡期：旧密钥签发的票据仍可通过");
        expect(ticket::verifyTicket(both, byKey2, fileId, "put", out),
               "过渡期：新密钥签发的票据可通过");

        // 退役旧公钥，只留新的
        std::vector<ticket::TicketKey> onlyNew = trustSet({pub2});
        expect(!ticket::verifyTicket(onlyNew, byKey1, fileId, "put", out),
               "退役后：旧密钥签发的票据被拒");
        expect(ticket::verifyTicket(onlyNew, byKey2, fileId, "put", out),
               "退役后：新密钥签发的票据仍可通过");
    }

    // ---- 畸形输入一律拒绝且不抛异常 ----
    {
        const char* bad[] = {
            "",
            "v2",
            "v1:0123456789abcdef0123456789abcdef:9999999999:1:1:put:aabb",
            "v2:short:9999999999:1:1:put:aabbcd",
            "v2:0123456789abcdef0123456789abcdef:9999999999:1:1:put:zzzz",
            "v2:0123456789ABCDEF0123456789abcdef:9999999999:1:1:put:aabbcd",
            "v2:0123456789abcdef0123456789abcdef:notanumber:1:1:put:aabbcd",
            "v2:0123456789abcdef0123456789abcdef:99999999999999999999999:1:1:put:aabbcd",
            "v2:0123456789abcdef0123456789abcdef:9999999999:0:1:put:aabbcd",
            "extra:v2:0123456789abcdef0123456789abcdef:9999999999:1:1:put:aabbcd",
        };
        int out = 0;
        bool allRejected = true;
        for (const char* b : bad) {
            if (ticket::verifyTicket(trust, b, fileId, "put", out)) {
                std::printf("  FAIL 畸形票据未被拒绝: [%s]\n", b);
                allRejected = false;
                ++g_failed;
            }
        }
        expect(allRejected, "全部畸形票据被拒且未抛异常");
    }

    // ---- 非 Ed25519 的密钥不得「加载成功」----
    {
        EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
        EVP_PKEY* rsa = nullptr;
        if (ctx != nullptr) {
            EVP_PKEY_keygen_init(ctx);
            EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048);
            EVP_PKEY_keygen(ctx, &rsa);
            EVP_PKEY_CTX_free(ctx);
        }
        if (rsa != nullptr) {
            expect(writePubPem(rsa, rsaPub), "写出 RSA 公钥（对照用）");
            ticket::TicketKey k;
            expect(!k.loadPublicPem(rsaPub), "非 Ed25519（RSA）公钥被拒绝加载");
            EVP_PKEY_free(rsa);
        } else {
            std::printf("  --   跳过 RSA 用例（本机无法生成 RSA 密钥）\n");
        }
    }
    {
        ticket::TicketKey k;
        expect(!k.loadPublicPem("/tmp/definitely_not_exist_ticket.pem"),
               "不存在的密钥路径加载失败（fail closed）");
        expect(!k.loadPrivatePem(""), "空路径加载失败");
    }

    // ---- 不可信来源数字解析的边界 ----
    {
        int v = 0;
        expect(parseNonNegativeInt("2147483647", v) && v == INT_MAX, "parse INT_MAX 通过");
        expect(!parseNonNegativeInt("2147483648", v), "parse INT_MAX+1 被拒（不抛异常）");
        expect(!parseNonNegativeInt("7000000847", v), "parse 7000000847（10 位但溢出）被拒");
        expect(!parseNonNegativeInt("", v) && !parseNonNegativeInt("abc", v) &&
                   !parseNonNegativeInt("-1", v),
               "parse 空串/非数字/负数被拒");
    }

    remove(priv1.c_str());
    remove(pub1.c_str());
    remove(pub2.c_str());
    remove(rsaPub.c_str());

    if (g_failed == 0) {
        std::printf("\n全部通过\n");
        return 0;
    }
    std::printf("\n失败 %d 项\n", g_failed);
    return 1;
}
