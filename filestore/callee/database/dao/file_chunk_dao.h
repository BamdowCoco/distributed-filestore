#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../Connection.hpp"

// file_chunk 表：每个文件块一行，记录它落在哪个存储节点、以及紧凑存储里的 (offset,size)。
class FileChunkDao
{
public:
    // 待写入的块位置（file_id 由调用方给出，不在这里存）
    struct Loc
    {
        int chunkIndex = 0;
        std::string ip;
        int port = 0;
    };

    // 从库里读出的块记录
    struct Row
    {
        int chunkIndex = 0;
        std::string ip;
        int port = 0;
        std::string checksum;
        int64_t offset = 0;
        int size = 0;
    };

    struct NodeAddr
    {
        std::string ip;
        int port = 0;
    };

    explicit FileChunkDao(Connection& conn) : m_conn(conn) {}

    // 批量插入块位置（单条多值 INSERT；空列表视为成功且不执行语句）
    bool insertMany(int fileId, const std::vector<Loc>& locs);

    // 登记某块的校验和/偏移/大小（上传提交时逐块调用）
    bool updateChunkMeta(int fileId, int chunkIndex, const std::string& checksum,
                         int64_t offset, int size);

    // 该文件已登记的块数（用于提交前核对块记录是否齐全）
    bool countByFile(int fileId, int& out) const;

    // 按块号顺序列出该文件的所有块
    bool listByFile(int fileId, std::vector<Row>& out) const;

    // 该文件涉及的去重存储节点列表（删除时用；原先 4 处重复的同一条 SQL）
    bool distinctNodes(int fileId, std::vector<NodeAddr>& out) const;

    bool deleteByFile(int fileId);

private:
    Connection& m_conn;
};
