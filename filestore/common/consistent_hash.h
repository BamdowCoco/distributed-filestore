#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "common.h"   // md5Hex

// 存储节点地址
struct StorageNode
{
    std::string ip;
    int port;
};

// 一致性哈希环：物理节点 -> K 个虚拟节点 -> [0, 2^32) 环
// 块分配时按 key 顺时针找到下一个虚拟节点对应的物理节点，
// 节点增删只影响环上相邻区间的块（约 1/N），而非取模的全量重映射。
class ConsistentHash
{
public:
    // 用节点集合构建环，每个物理节点生成 virtualNodes 个虚拟节点
    void build(const std::vector<StorageNode>& nodes, int virtualNodes = 150)
    {
        m_ring.clear();
        m_virtualNodes = virtualNodes;
        for (const StorageNode& node : nodes) {
            std::string nodeId = node.ip + ":" + std::to_string(node.port);
            for (int v = 0; v < virtualNodes; ++v) {
                uint32_t h = hashKey(nodeId + "#" + std::to_string(v));
                m_ring[h] = node;
            }
        }
    }

    // 根据 key 顺时针找到负责的物理节点
    StorageNode locate(const std::string& key) const
    {
        uint32_t h = hashKey(key);
        auto it = m_ring.upper_bound(h);
        if (it == m_ring.end()) {
            it = m_ring.begin();   // 环尾回绕到环首
        }
        return it->second;
    }

    bool empty() const { return m_ring.empty(); }

    // 哈希函数：复用 md5Hex 取前 4 字节，避免 std::hash 跨平台/进程不稳定
    static uint32_t hashKey(const std::string& key)
    {
        std::string md5 = md5Hex(key);   // 32 个十六进制字符
        return static_cast<uint32_t>(std::stoul(md5.substr(0, 8), nullptr, 16));
    }

private:
    std::map<uint32_t, StorageNode> m_ring;   // hash -> 物理节点
    int m_virtualNodes = 150;
};
