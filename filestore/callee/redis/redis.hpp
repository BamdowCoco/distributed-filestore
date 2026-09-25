#ifndef REDIS_H
#define REDIS_H

#include <hiredis/hiredis.h>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

class Redis
{
public:
    Redis();
    ~Redis();

    // 连接redis服务器
    bool connect();

    // 创建消费者组（幂等：组已存在也返回 true）
    bool xgroupCreate(const std::string& stream, const std::string& group);

    // 入队：向 Stream 追加一条 field=value 消息，返回消息 ID（失败返回空串）
    std::string xadd(const std::string& stream, const std::string& field, const std::string& value);

    // 消费（消费者组）：id=">" 读新消息，id="0" 读本消费者 pending 未确认消息。
    // value 在函数内转为 int 返回（任务 ID），避免调用方遗漏转换。
    // 返回 (消息ID, int value) 列表
    std::vector<std::pair<std::string, int>> xreadGroup(const std::string& stream,
                                                        const std::string& group,
                                                        const std::string& consumer,
                                                        const std::string& id, int count);

    // 确认：XACK，从 PEL 移除已处理消息
    bool xack(const std::string& stream, const std::string& group, const std::string& id);

    // 通用键值：SET key value EX ttl（ttl<=0 表示不过期）
    bool set(const std::string& key, const std::string& value, int ttlSeconds = 0);

    // 通用键值：GET key，返回 value（键不存在返回空串）
    std::string get(const std::string& key);

    // 通用键值：DEL key，返回是否删除成功
    bool del(const std::string& key);

private:
    const char* REDIS_IP = "127.0.0.1";
    const int REDIS_PORT = 6379;

    redisContext* _ctx;   // hiredis 同步上下文
    // hiredis context 非线程安全，所有命令加锁保护
    std::mutex _mutex;
};

#endif
