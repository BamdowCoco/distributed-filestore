#include "redis.hpp"
#include <cstdlib>
#include <cstring>
#include <iostream>

using namespace std;

Redis::Redis() : _ctx(nullptr)
{
}

Redis::~Redis()
{
    if (_ctx) {
        redisFree(_ctx);
    }
}

// 连接redis服务器
bool Redis::connect()
{
    _ctx = redisConnect(REDIS_IP, REDIS_PORT);
    if (_ctx == nullptr) {
        cerr << "failed to connect redis!" << endl;
        return false;
    }
    if (_ctx->err) {
        cerr << "failed to connect redis: " << _ctx->errstr << endl;
        redisFree(_ctx);
        _ctx = nullptr;
        return false;
    }
    cout << "connect redis-server success!" << endl;
    return true;
}

// 创建消费者组（幂等：组已存在也返回 true）
bool Redis::xgroupCreate(const std::string& stream, const std::string& group)
{
    if (_ctx == nullptr) {
        return false;
    }
    lock_guard<mutex> lock(_mutex);
    redisReply* reply = (redisReply*)redisCommand(_ctx, "XGROUP CREATE %s %s $ MKSTREAM",
                                                  stream.c_str(), group.c_str());
    if (reply == nullptr) {
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    if (!ok && reply->str && strstr(reply->str, "BUSYGROUP") != nullptr) {
        ok = true;   // 组已存在，视为成功
    }
    freeReplyObject(reply);
    return ok;
}

// 入队：XADD stream * field value，返回消息 ID（失败返回空串）
std::string Redis::xadd(const std::string& stream, const std::string& field, const std::string& value)
{
    if (_ctx == nullptr) {
        return "";
    }
    lock_guard<mutex> lock(_mutex);
    redisReply* reply = (redisReply*)redisCommand(_ctx, "XADD %s * %s %s",
                                                  stream.c_str(), field.c_str(), value.c_str());
    if (reply == nullptr || reply->type != REDIS_REPLY_STRING) {
        if (reply) {
            freeReplyObject(reply);
        }
        return "";
    }
    std::string id = reply->str;
    freeReplyObject(reply);
    return id;
}

// 消费（消费者组）：XREADGROUP GROUP group consumer COUNT count STREAMS stream id
// value 在函数内转 int，返回 (消息ID, int) 列表
std::vector<std::pair<std::string, int>> Redis::xreadGroup(const std::string& stream,
                                                           const std::string& group,
                                                           const std::string& consumer,
                                                           const std::string& id, int count)
{
    std::vector<std::pair<std::string, int>> out;
    if (_ctx == nullptr) {
        return out;
    }
    lock_guard<mutex> lock(_mutex);
    redisReply* reply = (redisReply*)redisCommand(_ctx, "XREADGROUP GROUP %s %s COUNT %d STREAMS %s %s",
                                                  group.c_str(), consumer.c_str(), count,
                                                  stream.c_str(), id.c_str());
    if (reply == nullptr || reply->type != REDIS_REPLY_ARRAY) {
        if (reply) {
            freeReplyObject(reply);
        }
        return out;
    }

    // 响应结构：[ [streamName, [ [msgId, [field, value]], ... ]], ... ]
    for (size_t i = 0; i < reply->elements; ++i) {
        redisReply* streamReply = reply->element[i];
        if (streamReply->type != REDIS_REPLY_ARRAY || streamReply->elements < 2) {
            continue;
        }
        redisReply* entries = streamReply->element[1];
        if (entries->type != REDIS_REPLY_ARRAY) {
            continue;
        }
        for (size_t j = 0; j < entries->elements; ++j) {
            redisReply* entry = entries->element[j];
            if (entry->type != REDIS_REPLY_ARRAY || entry->elements < 2) {
                continue;
            }
            std::string msgId = (entry->element[0] && entry->element[0]->str) ? entry->element[0]->str : "";
            redisReply* fields = entry->element[1];
            int value = 0;
            if (fields->type == REDIS_REPLY_ARRAY && fields->elements >= 2 &&
                fields->element[1] && fields->element[1]->str) {
                value = atoi(fields->element[1]->str);   // value 是任务 ID（整型），函数内转换
            }
            out.emplace_back(msgId, value);
        }
    }
    freeReplyObject(reply);
    return out;
}

// 确认：XACK stream group id，从 PEL 移除已处理消息
bool Redis::xack(const std::string& stream, const std::string& group, const std::string& id)
{
    if (_ctx == nullptr) {
        return false;
    }
    lock_guard<mutex> lock(_mutex);
    redisReply* reply = (redisReply*)redisCommand(_ctx, "XACK %s %s %s",
                                                  stream.c_str(), group.c_str(), id.c_str());
    if (reply == nullptr) {
        return false;
    }
    long long acked = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : 0;
    freeReplyObject(reply);
    return acked > 0;
}

// 通用键值：SET key value [EX ttl]
bool Redis::set(const std::string& key, const std::string& value, int ttlSeconds)
{
    if (_ctx == nullptr) {
        return false;
    }
    lock_guard<mutex> lock(_mutex);
    redisReply* reply;
    if (ttlSeconds > 0) {
        reply = (redisReply*)redisCommand(_ctx, "SET %s %s EX %d",
                                          key.c_str(), value.c_str(), ttlSeconds);
    } else {
        reply = (redisReply*)redisCommand(_ctx, "SET %s %s",
                                          key.c_str(), value.c_str());
    }
    if (reply == nullptr) {
        return false;
    }
    bool ok = (reply->type == REDIS_REPLY_STATUS);
    freeReplyObject(reply);
    return ok;
}

// 通用键值：GET key
std::string Redis::get(const std::string& key)
{
    if (_ctx == nullptr) {
        return "";
    }
    lock_guard<mutex> lock(_mutex);
    redisReply* reply = (redisReply*)redisCommand(_ctx, "GET %s", key.c_str());
    if (reply == nullptr || reply->type != REDIS_REPLY_STRING) {
        if (reply) {
            freeReplyObject(reply);
        }
        return "";
    }
    std::string value = (reply->str != nullptr) ? reply->str : "";
    freeReplyObject(reply);
    return value;
}

// 通用键值：DEL key
bool Redis::del(const std::string& key)
{
    if (_ctx == nullptr) {
        return false;
    }
    lock_guard<mutex> lock(_mutex);
    redisReply* reply = (redisReply*)redisCommand(_ctx, "DEL %s", key.c_str());
    if (reply == nullptr) {
        return false;
    }
    long long deleted = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : 0;
    freeReplyObject(reply);
    return deleted > 0;
}
