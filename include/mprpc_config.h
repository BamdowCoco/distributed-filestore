#pragma once

#include <cstdint>
#include <unordered_map>
#include <string>

// 框架读取配置文件类
// rpc_server_ip rpc_server_port zookeeper_ip zookeeper_port
//
// 取值一律用下面几个带类型、带默认值的接口，**它们不会抛异常**：
// 配置写错（缺项、非数字、数值越界）时回退默认值，而不是在启动或某个请求里抛出去。
// 早期是在这个类**外面**散着若干"安全解析"补丁函数（每个调用点各写一份），
// 现收回来，避免语义漂移（位数上限、是否允许 0、是否查 INT_MAX 各不一致）。
class MprpcConfig
{
public:
    // 加载解析配置文件
    bool loadConfigFile(std::string configFile);
    // 查找对应key的配置信息（原始字符串；缺失返回空串）
    std::string load(const std::string& key) const;

    // 取字符串；缺失返回 defaultValue
    std::string getString(const std::string& key, const std::string& defaultValue = "") const;

    // 取非负整数（**允许 0**；0 常被用作"显式关闭某功能"，如 rpc_worker_threads=0）
    int getInt(const std::string& key, int defaultValue) const;

    // 取正整数（<=0 视为非法 → 回默认值）
    int getPositiveInt(const std::string& key, int defaultValue) const;

    // 取 [minValue, maxValue] 区间内的整数；缺失/非法/越界一律回默认值。
    // **用于"数量类"配置**（线程数、连接数）——这类值即使语法合法，过大也会在运行时炸：
    // 例如 rpc_worker_threads=70000 会让线程池真的去创建 7 万个线程，
    // std::thread 抛 std::system_error 且无人捕获 → 进程 abort。
    int getIntInRange(const std::string& key, int defaultValue, int minValue, int maxValue) const;

    // 取 int64（如超时毫秒数）
    int64_t getInt64(const std::string& key, int64_t defaultValue) const;

private:
    std::unordered_map<std::string, std::string> m_configMap;

    // 去除空格字符
    void removeSpace(std::string& str);
};