// 口令散列模块（filestore/common/password.h）的单元测试。
// 不依赖 ZooKeeper / MySQL / 服务进程。
// 编译：g++ -O2 -std=c++11 -I filestore/common test_password.cc -o test_password -lcrypto
#include <cstdio>
#include <string>

#include "../../filestore/common/password.h"

static int g_failed = 0;

static void expect(bool cond, const char* what)
{
    std::printf("  %s %s\n", cond ? "ok  " : "FAIL", what);
    if (!cond) {
        ++g_failed;
    }
}

int main()
{
    using password::VerifyResult;
    std::printf("===== 口令散列单元测试 =====\n");

    const std::string pw = "correct horse battery staple";

    // 1. 生成与校验
    std::string stored = password::hashPassword(pw);
    expect(!stored.empty(), "hashPassword 返回非空");
    expect(stored.compare(0, 14, "pbkdf2-sha256$") == 0, "格式前缀正确（算法可识别）");
    expect(password::verifyPassword(pw, stored) == VerifyResult::Ok,
           "正确口令校验通过（Ok）");
    expect(password::verifyPassword("wrong password", stored) == VerifyResult::Mismatch,
           "错误口令被拒（Mismatch）");
    expect(password::verifyPassword("", stored) == VerifyResult::Mismatch,
           "空口令被拒");

    // 2. 盐必须随机：同一口令两次散列结果不同，但都能通过校验
    std::string stored2 = password::hashPassword(pw);
    expect(stored != stored2, "同一口令两次散列的存储值不同（盐是随机的）");
    expect(password::verifyPassword(pw, stored2) == VerifyResult::Ok, "第二个散列同样可校验");

    // 3. 历史遗留格式：无盐 SHA256 仍可登录，但会被标记为需要升级
    std::string legacy = sha256Hex(pw);
    expect(password::verifyPassword(pw, legacy) == VerifyResult::OkNeedsRehash,
           "旧的无盐 SHA256 可通过但标记 OkNeedsRehash（触发升级）");
    expect(password::verifyPassword("wrong", legacy) == VerifyResult::Mismatch,
           "旧的错误口令被拒");

    // 4. 篡改：改掉散列体或盐必须失败（不得抛异常）
    {
        std::string t = stored;
        t[t.size() - 1] = (t[t.size() - 1] == 'A') ? 'B' : 'A';
        expect(password::verifyPassword(pw, t) == VerifyResult::Mismatch,
               "篡改散列体后被拒");
    }
    {
        // 把盐段换成另一个合法 base64（等长）→ 也必须失败
        std::string t = stored;
        size_t first = t.find('$');
        size_t second = t.find('$', first + 1);
        size_t third = t.find('$', second + 1);
        for (size_t i = second + 1; i < third; ++i) {
            t[i] = (t[i] == 'A') ? 'B' : 'A';
        }
        expect(password::verifyPassword(pw, t) == VerifyResult::Mismatch,
               "篡改盐后被拒");
    }

    // 5. 畸形存储值一律按失败处理，且**不得抛异常**
    const char* bad[] = {
        "",
        "abc",
        "pbkdf2-sha256$",
        "pbkdf2-sha256$100000$",
        "pbkdf2-sha256$100000$c2FsdA==$",
        "pbkdf2-sha256$notanumber$c2FsdA==$aGFzaA==",
        "pbkdf2-sha256$0$c2FsdA==$aGFzaA==",
        "pbkdf2-sha256$99999999999$c2FsdA==$aGFzaA==",   // 迭代数超大：须被上界挡住
        "pbkdf2-sha256$100000$!!!!$aGFzaA==",             // 非法 base64
        "pbkdf2-sha256$100000$c2FsdA==$aGFzaA==",         // 合法 base64 但散列长度不对
        "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",  // 64 位但非 hex
        "$100000$c2FsdA==$aGFzaA==",
    };
    bool allRejected = true;
    for (const char* b : bad) {
        if (password::verifyPassword(pw, b) != VerifyResult::Mismatch) {
            std::printf("  FAIL 畸形值未被拒绝: [%s]\n", b);
            allRejected = false;
            ++g_failed;
        }
    }
    expect(allRejected, "全部畸形存储值被拒且未抛异常");

    if (g_failed == 0) {
        std::printf("\n全部通过\n");
        return 0;
    }
    std::printf("\n失败 %d 项\n", g_failed);
    return 1;
}
