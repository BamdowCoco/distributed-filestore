// 共享辅助函数的单元测试（filestore/common/ticket.h 与 common.h）
// 不依赖 MySQL/ZooKeeper/Redis，可离线运行：g++ -O2 -std=c++11 test_ticket.cc -lcrypto
#include <cassert>
#include <climits>
#include <cstdint>
#include <iostream>
#include <string>

#include "../../filestore/common/common.h"
#include "../../filestore/common/ticket.h"

static int g_failed = 0;

static void expect(bool cond, const std::string& what)
{
    if (!cond) {
        std::cout << "FAIL: " << what << std::endl;
        ++g_failed;
    } else {
        std::cout << "ok  : " << what << std::endl;
    }
}

int main()
{
    // 单元测试用的任意密钥材料，不是真实凭据
    const std::string secret = "unit-test-key-not-a-credential";
    const int userId = 42;
    const int32_t fileId = 1001;
    int out = 0;

    // 1. 正常签发与验签
    std::string t = makeTicket(secret, userId, fileId, "put", 60);
    expect(verifyTicket(secret, t, fileId, "put", out), "合法票据通过验签");
    expect(out == userId, "验签返回签发时的 userId");

    // 2. 操作绑定：put 票据不能用于 get/del/list
    expect(!verifyTicket(secret, t, fileId, "get", out), "拒绝跨操作使用（get）");
    expect(!verifyTicket(secret, t, fileId, "del", out), "拒绝跨操作使用（del）");
    expect(!verifyTicket(secret, t, fileId, "list", out), "拒绝跨操作使用（list）");

    // 3. file_id 绑定：不能拿 A 文件的票据去读写 B 文件
    expect(!verifyTicket(secret, t, fileId + 1, "put", out), "拒绝跨 file_id 使用");

    // 4. 密钥绑定：换密钥即失效
    expect(!verifyTicket("other_secret", t, fileId, "put", out), "错误密钥验签失败");

    // 5. 篡改任意一段即失效
    std::string tampered = t;
    tampered[tampered.size() - 1] = (tampered[tampered.size() - 1] == 'a') ? 'b' : 'a';
    expect(!verifyTicket(secret, tampered, fileId, "put", out), "篡改签名后验签失败");

    // 篡改 file_id 段（不改签名）也要被发现：把 fileId 换成另一值
    {
        std::string fake = makeTicket(secret, userId, fileId + 7, "put", 60);
        expect(!verifyTicket(secret, fake, fileId, "put", out), "签名覆盖 file_id，改号即失效");
    }

    // 6. 过期
    std::string expired = makeTicket(secret, userId, fileId, "put", -1);
    expect(!verifyTicket(secret, expired, fileId, "put", out), "过期票据被拒绝");

    // 7. 空值 / 畸形输入不得通过，也不得抛异常
    expect(!verifyTicket(secret, "", fileId, "put", out), "空票据被拒绝");
    expect(!verifyTicket("", t, fileId, "put", out), "空密钥一律拒绝");
    expect(!verifyTicket(secret, "abc", fileId, "put", out), "段数不足被拒绝");
    expect(!verifyTicket(secret, "1:2:3:put", fileId, "put", out), "段数不足（4 段）被拒绝");
    expect(!verifyTicket(secret, "1:2:3:put:x:y", fileId, "put", out), "段数过多被拒绝");
    expect(!verifyTicket(secret, "99999999999999999999:1:3:put:aa", fileId, "put", out),
           "超长数字段被拒绝（不抛异常）");
    expect(!verifyTicket(secret, "notanumber:1:3:put:aa", fileId, "put", out),
           "非数字段被拒绝（不抛异常）");

    // 8. ListFiles 场景：file_id=0 的非文件维度票据
    std::string listTicket = makeTicket(secret, 0, 0, "list", 60);
    expect(verifyTicket(secret, listTicket, 0, "list", out), "非文件维度票据（file_id=0）可用");
    expect(!verifyTicket(secret, listTicket, 5, "list", out), "非文件维度票据不能用于具体文件");

    // 9. parseNonNegativeInt：处理不可信来源的数字，必须不抛异常且拦得住溢出
    int v = 0;
    expect(parseNonNegativeInt("0", v) && v == 0, "parse \"0\" -> 0");
    expect(parseNonNegativeInt("007", v) && v == 7, "parse \"007\" -> 7（允许前导零）");
    expect(parseNonNegativeInt("2147483647", v) && v == INT_MAX, "parse INT_MAX 通过");
    // 下面这条是关键回归：10 位数也可能超过 INT_MAX，仅靠位数判断拦不住
    expect(!parseNonNegativeInt("2147483648", v), "parse INT_MAX+1 被拒（不抛异常）");
    expect(!parseNonNegativeInt("7000000847", v),
           "parse 7000000847（10 位但溢出）被拒（不抛异常）");
    expect(!parseNonNegativeInt("9999999999", v), "parse 10 位最大值被拒");
    expect(!parseNonNegativeInt("12345678901", v), "parse 11 位被拒");
    expect(!parseNonNegativeInt("", v), "parse 空串被拒");
    expect(!parseNonNegativeInt("abc", v), "parse 非数字被拒");
    expect(!parseNonNegativeInt("12a", v), "parse 混合字符被拒");
    expect(!parseNonNegativeInt("-1", v), "parse 负数被拒");

    if (g_failed == 0) {
        std::cout << "\n全部通过" << std::endl;
        return 0;
    }
    std::cout << "\n失败 " << g_failed << " 项" << std::endl;
    return 1;
}
