#include "mprpc_config.h"
#include "logger.h"

#include <fstream>
#include <cctype>
#include <climits>
#include <cstdint>

// 加载解析配置文件
bool MprpcConfig::loadConfigFile(std::string configFile)
{
    // 打开文件
    std::ifstream inFile(configFile);
    if(!inFile)
    {
        LOG("failed to open file:" + configFile);
        return false;
    }

    // 逐行读取文件
    std::string line;
    while (std::getline(inFile, line)) {
        // 跳过空行和注释
        if(line.empty() || line[0]=='#' || line[0]=='[') {
            continue;
        }
        // 去除空白字符
        removeSpace(line);
        // 读取key value
        int idx = line.find('=');
        std::string key = line.substr(0, idx);
        std::string value = line.substr(idx+1);
        // 加入 configmap
        m_configMap.insert({key, value});
    }
    // 关闭文件
    inFile.close();
    return true;
}

// 查找对应key的配置信息
std::string MprpcConfig::load(const std::string& key) const
{
    auto it = m_configMap.find(key);
    if(it == m_configMap.end()) {
        return {};
    }
    return it->second;
}

// 去除空格字符
void MprpcConfig::removeSpace(std::string& str)
{
    int index = 0;
    for(int i=0;i<str.size();i++) {
        if(!std::isspace(static_cast<unsigned char>(str[i]))) {
            str[index++] = str[i];
        }
    }
    str.resize(index);
}

namespace {

// 把纯十进制非负整数字符串解析为 int64，超过 limit 或含非法字符即失败。
// 手写累加而不用 std::stoi/std::stoll：后者对畸形/超长输入会抛异常，而配置取值绝不该抛。
// 位数上限（18）保证累加过程不会溢出 int64，之后再逐位与 limit 比较。
bool parseDecimal(const std::string& s, int64_t limit, int64_t& out)
{
    if (s.empty() || s.size() > 18) {
        return false;
    }
    int64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') {
            return false;   // 含符号、空白或其它字符一律非法
        }
        v = v * 10 + (c - '0');
        if (v > limit) {
            return false;
        }
    }
    out = v;
    return true;
}

}  // namespace

// 取字符串；缺失返回 defaultValue
std::string MprpcConfig::getString(const std::string& key, const std::string& defaultValue) const
{
    std::string v = load(key);
    return v.empty() ? defaultValue : v;
}

// 取非负整数（允许 0）
int MprpcConfig::getInt(const std::string& key, int defaultValue) const
{
    int64_t v = 0;
    if (!parseDecimal(load(key), INT_MAX, v)) {
        return defaultValue;
    }
    return static_cast<int>(v);   // 已保证 <= INT_MAX
}

// 取正整数（<=0 视为非法）
int MprpcConfig::getPositiveInt(const std::string& key, int defaultValue) const
{
    int64_t v = 0;
    if (!parseDecimal(load(key), INT_MAX, v) || v <= 0) {
        return defaultValue;
    }
    return static_cast<int>(v);
}

// 取 [minValue, maxValue] 区间内的整数；缺失/非法/越界一律回默认值
int MprpcConfig::getIntInRange(const std::string& key, int defaultValue,
                               int minValue, int maxValue) const
{
    int64_t v = 0;
    if (!parseDecimal(load(key), INT_MAX, v) || v < minValue || v > maxValue) {
        return defaultValue;
    }
    return static_cast<int>(v);
}

// 取 int64
int64_t MprpcConfig::getInt64(const std::string& key, int64_t defaultValue) const
{
    int64_t v = 0;
    if (!parseDecimal(load(key), INT64_MAX, v)) {
        return defaultValue;
    }
    return v;
}