#include "storage_service.h"

#include <cerrno>
#include <cstdio>
#include <dirent.h>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

#include "common/common.h"
#include "common/ticket.h"
#include "logger.h"
#include "mprpc_application.h"

// 构造：读取 data_dir 与票据密钥，并确保数据目录存在
StorageService::StorageService()
{
    m_dataDir = MprpcApplication::getConfig().load("data_dir");
    // 确保数据目录存在（忽略已存在的错误）
    mkdir(m_dataDir.c_str(), 0755);

    // 可信票据公钥：Ed25519 的 PEM 文件**路径**，可逗号分隔多把（用于手动轮换）。
    // 公钥不是机密——写进配置文件本来就安全，这正是改用非对称签名的收益之一：
    // 需要保密的只有元数据服务那一把私钥。
    std::string pubPaths = MprpcApplication::getConfig().load("ticket_pubkey");
    const char* envPubPath = std::getenv("MPRPC_TICKET_PUBKEY");
    if (envPubPath != nullptr && *envPubPath != '\0') {
        pubPaths = envPubPath;
    }
    {
        std::stringstream ss(pubPaths);
        std::string item;
        while (std::getline(ss, item, ',')) {
            size_t begin = item.find_first_not_of(" \t");
            if (begin == std::string::npos) {
                continue;   // 空项（如结尾多余的逗号）
            }
            size_t end = item.find_last_not_of(" \t");
            std::string path = item.substr(begin, end - begin + 1);
            ticket::TicketKey key;
            if (!key.loadPublicPem(path)) {
                // 单把加载失败不算致命：可能是轮换期间某把已下线，其余仍可用
                LOG_ERROR("failed to load ticket public key, skipped. path:%s", path.c_str());
                continue;
            }
            m_ticketKeys.push_back(std::move(key));
        }
    }
    if (m_ticketKeys.empty()) {
        // 一把可信公钥都没有就什么都验不过：显式失败退出，不要带着空集合对外服务
        LOG_ERROR("no usable storage ticket public key; set ticket_pubkey in the config file "
                  "(or MPRPC_TICKET_PUBKEY) to one or more Ed25519 PEM paths. configured:%s",
                  pubPaths.c_str());
        exit(EXIT_FAILURE);
    }
    LOG_INFO("storage ticket keys loaded, count:%zu", m_ticketKeys.size());
}

// 上传单块：紧凑追加写到 data_dir/<file_id> 末尾，返回 (offset, size, checksum)
void StorageService::PutChunk(::google::protobuf::RpcController* controller,
                              const ::filestore::PutChunkRequest* request,
                              ::filestore::PutChunkResponse* response,
                              ::google::protobuf::Closure* done)
{
    if (!checkTicket(request->file_id(), "put", request->ticket())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid or expired ticket");
        done->Run();
        return;
    }
    if (!isValidFileId(request->file_id())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid file_id");
        done->Run();
        return;
    }

    std::string path = dataPath(request->file_id());

    FILE* fp = fopen(path.c_str(), "r+b");
    if (fp == nullptr) {
        // 首次写入，创建文件
        fp = fopen(path.c_str(), "wb");
    }
    if (fp == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("failed to open file for write");
        done->Run();
        return;
    }

    // 定位到文件末尾，紧凑追加写。fseek/ftell/fwrite 的返回值都要检查：
    // 磁盘满或配额超限时 fwrite 会少写，若不检查就会返回成功并附上一个
    // 与实际落盘内容不符的 checksum，元数据据此登记出一个损坏的文件。
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("seek to end failed");
        done->Run();
        return;
    }
    int64_t offset = static_cast<int64_t>(ftell(fp));
    if (offset < 0) {
        fclose(fp);
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("tell failed");
        done->Run();
        return;
    }
    int32_t size = static_cast<int32_t>(request->data().size());
    if (!request->data().empty() &&
        fwrite(request->data().data(), 1, request->data().size(), fp) != request->data().size()) {
        // 回退到本次写入前的位置，避免在文件尾部留下半块垃圾（客户端会重试整批）
        ftruncate(fileno(fp), static_cast<off_t>(offset));
        fclose(fp);
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("write chunk failed");
        done->Run();
        return;
    }
    fclose(fp);

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_checksum(md5Hex(request->data()));
    response->set_offset(offset);
    response->set_size(size);
    LOG_INFO("put chunk file_id:%d idx:%d offset:%lld size:%d checksum:%s",
             request->file_id(), request->chunk_index(),
             static_cast<long long>(offset), size, response->checksum().c_str());

    done->Run();
}

// 下载单块：按紧凑存储记录的 (offset, size) 定位读取
void StorageService::GetChunk(::google::protobuf::RpcController* controller,
                              const ::filestore::GetChunkRequest* request,
                              ::filestore::GetChunkResponse* response,
                              ::google::protobuf::Closure* done)
{
    if (!checkTicket(request->file_id(), "get", request->ticket())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid or expired ticket");
        done->Run();
        return;
    }
    if (!isValidFileId(request->file_id())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid file_id");
        done->Run();
        return;
    }
    // 读取范围由客户端给出，必须有界：负 size 会让 std::string 构造抛 length_error，
    // 异常逃出 muduo 工作线程会直接终止进程；超大 size 则是无上限的内存分配。
    if (request->offset() < 0 || request->size() < 0 || request->size() > CHUNK_SIZE) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid offset or size");
        done->Run();
        return;
    }

    std::string path = dataPath(request->file_id());

    FILE* fp = fopen(path.c_str(), "rb");
    if (fp == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("file not found");
        done->Run();
        return;
    }

    if (fseek(fp, static_cast<long>(request->offset()), SEEK_SET) != 0) {
        fclose(fp);
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("seek failed");
        done->Run();
        return;
    }
    std::string buf(request->size(), '\0');
    size_t nread = 0;
    if (request->size() > 0) {
        nread = fread(&buf[0], 1, request->size(), fp);
        if (nread != static_cast<size_t>(request->size())) {
            // 短读说明元数据记录的 (offset,size) 与实际数据文件不符，不能当作正常响应返回
            fclose(fp);
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("read chunk failed");
            done->Run();
            return;
        }
    }
    fclose(fp);

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    response->set_data(buf);
    LOG_INFO("get chunk file_id:%d idx:%d offset:%lld size:%zu",
             request->file_id(), request->chunk_index(),
             static_cast<long long>(request->offset()), nread);

    done->Run();
}

// 批量上传：一次请求紧凑追加写同节点多块，逐块返回 (offset, size, checksum)
void StorageService::PutChunksBatch(::google::protobuf::RpcController* controller,
                                    const ::filestore::PutChunksBatchRequest* request,
                                    ::filestore::PutChunksBatchResponse* response,
                                    ::google::protobuf::Closure* done)
{
    if (!checkTicket(request->file_id(), "put", request->ticket())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid or expired ticket");
        done->Run();
        return;
    }
    if (!isValidFileId(request->file_id())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid file_id");
        done->Run();
        return;
    }

    std::string path = dataPath(request->file_id());

    FILE* fp = fopen(path.c_str(), "r+b");
    if (fp == nullptr) {
        fp = fopen(path.c_str(), "wb");
    }
    if (fp == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("failed to open file for write");
        done->Run();
        return;
    }

    // 顺序紧凑追加写，逐块返回 (offset, size, checksum)。
    // 全程检查 fseek/ftell/fwrite：写失败时若仍返回成功，客户端会把一个
    // 内容损坏的文件提交进元数据，且录入的 checksum 与实际落盘内容不符。
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("seek to end failed");
        done->Run();
        return;
    }
    int64_t batchStart = static_cast<int64_t>(ftell(fp));
    if (batchStart < 0) {
        fclose(fp);
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("tell failed");
        done->Run();
        return;
    }

    for (int i = 0; i < request->chunks_size(); ++i) {
        const auto& chunk = request->chunks(i);
        int64_t offset = static_cast<int64_t>(ftell(fp));
        if (offset < 0) {
            ftruncate(fileno(fp), static_cast<off_t>(batchStart));
            fclose(fp);
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("tell failed");
            done->Run();
            return;
        }
        int32_t size = static_cast<int32_t>(chunk.data().size());
        if (!chunk.data().empty() &&
            fwrite(chunk.data().data(), 1, chunk.data().size(), fp) != chunk.data().size()) {
            // 整批回退到批首：客户端会重试整批，留下半块会让重试后的数据文件夹带垃圾
            ftruncate(fileno(fp), static_cast<off_t>(batchStart));
            fclose(fp);
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("write chunks batch failed");
            done->Run();
            return;
        }

        filestore::ChunkResult* r = response->add_chunks();
        r->set_chunk_index(chunk.chunk_index());
        r->set_offset(offset);
        r->set_size(size);
        r->set_checksum(md5Hex(chunk.data()));
    }
    fclose(fp);

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    LOG_INFO("put chunks batch file_id:%d count:%d", request->file_id(), request->chunks_size());

    done->Run();
}

// 批量下载：按 (offset, size) 定位读取同节点多块
void StorageService::GetChunksBatch(::google::protobuf::RpcController* controller,
                                    const ::filestore::GetChunksBatchRequest* request,
                                    ::filestore::GetChunksBatchResponse* response,
                                    ::google::protobuf::Closure* done)
{
    if (!checkTicket(request->file_id(), "get", request->ticket())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid or expired ticket");
        done->Run();
        return;
    }
    if (!isValidFileId(request->file_id())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid file_id");
        done->Run();
        return;
    }

    std::string path = dataPath(request->file_id());

    FILE* fp = fopen(path.c_str(), "rb");
    if (fp == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("file not found");
        done->Run();
        return;
    }

    // 逐块按 (offset, size) 定位读取。范围由客户端给出，必须逐块校验（理由同 GetChunk）
    for (int i = 0; i < request->chunks_size(); ++i) {
        const auto& spec = request->chunks(i);
        if (spec.offset() < 0 || spec.size() < 0 || spec.size() > CHUNK_SIZE) {
            fclose(fp);
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("invalid offset or size");
            done->Run();
            return;
        }
        if (fseek(fp, static_cast<long>(spec.offset()), SEEK_SET) != 0) {
            fclose(fp);
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("seek failed");
            done->Run();
            return;
        }
        std::string buf(spec.size(), '\0');
        if (spec.size() > 0) {
            size_t nread = fread(&buf[0], 1, spec.size(), fp);
            if (nread != static_cast<size_t>(spec.size())) {
                fclose(fp);
                response->mutable_result()->set_errcode(1);
                response->mutable_result()->set_errmsg("read chunk failed");
                done->Run();
                return;
            }
        }

        filestore::GetChunkData* d = response->add_chunks();
        d->set_chunk_index(spec.chunk_index());
        d->set_data(buf);
    }
    fclose(fp);

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    LOG_INFO("get chunks batch file_id:%d count:%d", request->file_id(), request->chunks_size());

    done->Run();
}

// 删除本节点存储的文件数据（data_dir/<file_id>）
void StorageService::DeleteFile(::google::protobuf::RpcController* controller,
                                const ::filestore::DeleteFileRequest* request,
                                ::filestore::DeleteFileResponse* response,
                                ::google::protobuf::Closure* done)
{
    if (!checkTicket(request->file_id(), "del", request->ticket())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid or expired ticket");
        done->Run();
        return;
    }
    if (!isValidFileId(request->file_id())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid file_id");
        done->Run();
        return;
    }

    std::string path = dataPath(request->file_id());
    if (std::remove(path.c_str()) == 0) {
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        LOG_INFO("delete file_id:%d", request->file_id());
    } else if (errno == ENOENT) {
        // 文件本就不存在：删除的目标已达成，按成功处理（幂等删除）。
        // 若把「不存在」当失败，上传回滚时那些压根没落盘的块会让待清理队列
        // 无谓地反复重试并最终升级为 status=3（需人工），把正常流程变成告警。
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        LOG_INFO("delete file_id:%d skipped (already absent)", request->file_id());
    } else {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("remove failed");
    }
    done->Run();
}

// 列出 data_dir 下的 file_id（按分片过滤，供孤儿块 GC 扫描）
void StorageService::ListFiles(::google::protobuf::RpcController* controller,
                               const ::filestore::ListFilesRequest* request,
                               ::filestore::ListFilesResponse* response,
                               ::google::protobuf::Closure* done)
{
    // ListFiles 不是文件维度的操作，票据以 file_id=0 绑定 op=list（仅元数据服务的 GC 持有）
    if (!checkTicket(0, "list", request->ticket())) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("invalid or expired ticket");
        done->Run();
        return;
    }
    int shard = request->shard();
    int shardCount = request->shard_count();

    DIR* dir = opendir(m_dataDir.c_str());
    if (dir == nullptr) {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("failed to open data dir");
        done->Run();
        return;
    }

    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        // 跳过 . / .. / 隐藏文件（数据文件都是纯数字 file_id）
        if (entry->d_name[0] == '.') {
            continue;
        }
        // 分片过滤：只返回本分片的文件（分片哈希与 ConsistentHash::hashKey 同算法）
        if (shardCount > 1) {
            uint32_t h = static_cast<uint32_t>(std::stoul(md5Hex(entry->d_name).substr(0, 8), nullptr, 16));
            if (static_cast<int>(h % static_cast<uint32_t>(shardCount)) != shard) {
                continue;
            }
        }
        response->add_filenames(entry->d_name);
    }
    closedir(dir);

    done->Run();
}

// 校验 file_id 合法性（>0）
bool StorageService::isValidFileId(int32_t file_id)
{
    return file_id > 0;
}

// 校验存储访问票据：由元数据服务用 Ed25519 私钥签发，绑定本次的 file_id 与操作，
// 本节点只用公钥验签（拿不到签发能力）。存储节点直连暴露端口，没有这层校验时
// 任何能访问端口的人都能读写删任意 file_id。
bool StorageService::checkTicket(int32_t file_id, const std::string& op,
                                 const std::string& ticket) const
{
    int userId = 0;
    if (ticket::verifyTicket(m_ticketKeys, ticket, file_id, op, userId)) {
        return true;
    }
    LOG_ERROR("storage ticket rejected! file_id:%d op:%s", file_id, op.c_str());
    return false;
}

// 数据文件路径：data_dir/<file_id>
std::string StorageService::dataPath(int32_t file_id) const
{
    return m_dataDir + "/" + std::to_string(file_id);
}
