// 指标2：块重映射 ~1/N（一致性哈希 vs 取模）
// 对同一批块 key，统计「新增 1 个节点」前后归属发生变化的块占比，对比取模算法。
#include <iostream>
#include <string>
#include <vector>

#include "consistent_hash.h"

static std::string nodeId(const StorageNode& n)
{
    return n.ip + ":" + std::to_string(n.port);
}

int main()
{
    const int KEY_COUNT = 10000;

    // 初始 3 个节点 -> 新增 1 个节点，共 4 个
    std::vector<StorageNode> before = {
        {"127.0.0.1", 8001},
        {"127.0.0.1", 8002},
        {"127.0.0.1", 8003},
    };
    std::vector<StorageNode> after = before;
    after.push_back({"127.0.0.1", 8004});

    ConsistentHash chBefore, chAfter;
    chBefore.build(before);
    chAfter.build(after);

    // 一致性哈希：统计增节点前后归属变化的块数
    int chMoved = 0;
    for (int i = 0; i < KEY_COUNT; ++i) {
        std::string key = "file#" + std::to_string(i);
        if (nodeId(chBefore.locate(key)) != nodeId(chAfter.locate(key))) {
            ++chMoved;
        }
    }

    // 取模：i % 3 vs i % 4
    int modMoved = 0;
    for (int i = 0; i < KEY_COUNT; ++i) {
        if (i % 3 != i % 4) {
            ++modMoved;
        }
    }

    std::cout << "===== 指标2：块重映射（一致性哈希 vs 取模） =====" << std::endl;
    std::cout << "块 key 数量 = " << KEY_COUNT << std::endl;
    std::cout << "节点数变化：3 -> 4（新增 1 个节点）" << std::endl;
    std::cout << std::endl;
    std::cout << "[一致性哈希] 重映射块数 = " << chMoved
              << "，占比 = " << (100.0 * chMoved / KEY_COUNT) << "%" << std::endl;
    std::cout << "[取模]       重映射块数 = " << modMoved
              << "，占比 = " << (100.0 * modMoved / KEY_COUNT) << "%" << std::endl;
    std::cout << std::endl;
    std::cout << "结论：一致性哈希重映射约 1/N（此处 ≈1/4），取模接近全量重映射（此处 ≈3/4）。" << std::endl;
    return 0;
}
