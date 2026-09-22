// 指标4：空间利用率（固定偏移写 vs 紧凑追加写）
// 用一致性哈希分配块，得到同节点稀疏块号，真实写两个文件对比落盘大小，
// 验证紧凑追加写消除「固定偏移写」产生的存储空洞。
#include <iostream>
#include <string>
#include <vector>
#include <cstdio>
#include <sys/stat.h>

#include "consistent_hash.h"

static std::string nodeId(const StorageNode& n)
{
    return n.ip + ":" + std::to_string(n.port);
}

int main()
{
    const int CHUNK_COUNT = 1000;

    std::vector<StorageNode> nodes = {{"127.0.0.1", 8001}, {"127.0.0.1", 8002}};
    ConsistentHash ring;
    ring.build(nodes);

    std::string target = nodeId(nodes[0]);

    // 收集节点 1 负责的块号（一致性哈希下块号稀疏、乱序）
    std::vector<int> indexes;
    for (int i = 0; i < CHUNK_COUNT; ++i) {
        std::string key = "file#" + std::to_string(i);
        if (nodeId(ring.locate(key)) == target) {
            indexes.push_back(i);
        }
    }

    std::string data(CHUNK_SIZE, 'a');

    std::string offsetPath = "test/bench/build/test_offset.dat";
    std::string compactPath = "test/bench/build/test_compact.dat";

    // 固定偏移写：offset = chunk_index * CHUNK_SIZE
    {
        FILE* fp = fopen(offsetPath.c_str(), "wb");
        for (int idx : indexes) {
            fseek(fp, static_cast<long>(idx) * CHUNK_SIZE, SEEK_SET);
            fwrite(data.data(), 1, CHUNK_SIZE, fp);
        }
        fclose(fp);
    }

    // 紧凑追加写：顺序连续写
    {
        FILE* fp = fopen(compactPath.c_str(), "wb");
        for (size_t k = 0; k < indexes.size(); ++k) {
            fwrite(data.data(), 1, CHUNK_SIZE, fp);
        }
        fclose(fp);
    }

    struct stat so, sc;
    stat(offsetPath.c_str(), &so);
    stat(compactPath.c_str(), &sc);

    long long offsetSize = static_cast<long long>(so.st_size);
    long long compactSize = static_cast<long long>(sc.st_size);
    long long hole = offsetSize - compactSize;
    double ratio = (offsetSize > 0) ? (100.0 * hole / offsetSize) : 0.0;

    std::cout << "===== 指标4：空间利用率（固定偏移写 vs 紧凑追加写） =====" << std::endl;
    std::cout << "节点1 负责块数 = " << indexes.size() << "（来自一致性哈希分配，块号稀疏）" << std::endl;
    std::cout << "固定偏移写落盘大小 = " << offsetSize << " 字节" << std::endl;
    std::cout << "紧凑追加写落盘大小 = " << compactSize << " 字节" << std::endl;
    std::cout << "存储空洞 = " << hole << " 字节，空间利用率提升 = " << ratio << "%" << std::endl;
    std::cout << std::endl;
    std::cout << "说明：一致性哈希下同节点的块号稀疏，固定偏移写会在文件内留下大量空洞，" << std::endl;
    std::cout << "      紧凑追加写落盘大小 = 实际写入字节之和，无空洞。" << std::endl;

    // 清理临时文件
    remove(offsetPath.c_str());
    remove(compactPath.c_str());
    return 0;
}
