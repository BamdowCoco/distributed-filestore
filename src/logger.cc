#include "logger.h"
#include <thread>
#include <ctime>
#include <fstream>
#include <iostream>
#include <cstdlib>
#include <sys/stat.h>
#include <unistd.h>

// 读取当前进程名，用于日志文件命名（如 callee、caller）
static std::string getProcName()
{
    std::ifstream comm("/proc/self/comm");
    std::string name;
    std::getline(comm, name);
    return name;
}

std::string Logger::levelToString(Level level)
{
    static constexpr std::string_view names[] = {
        "INFO", "ERROR"
    };
    int idx = static_cast<int>(level);
    if(idx<0 || idx>=static_cast<int>(std::size(names))) {
        return "UNKNOWN";
    }
    return std::string(names[idx]);
}

Logger::Logger()
{
    // 启动写日志线程
    std::thread writeLogTask([this]() {
        // 确保 logs/ 目录存在
        mkdir("logs", 0755);

        // 程序名用于日志文件命名（如 callee、caller）
        std::string procName = getProcName();

        while (true) {
            // 转换为tm结构体
            std::time_t now = time(nullptr);
            std::tm now_tm;
            localtime_r(&now, &now_tm);

            char filePath[160];
            snprintf(filePath, sizeof(filePath),
                     "logs/%d-%d-%d_%s.log",
                     now_tm.tm_year + 1900,
                     now_tm.tm_mon + 1,
                     now_tm.tm_mday,
                     procName.c_str());

            char timeBuf[16];
            snprintf(timeBuf, sizeof(timeBuf), "%02d:%02d:%02d",
                     now_tm.tm_hour, now_tm.tm_min, now_tm.tm_sec);

            // 批量取出当前所有日志
            std::queue<std::string> logs;
            m_lockQue.drain(logs);

            if(logs.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            // 以末尾追加的模式 打开文件
            std::ofstream file(filePath, std::ios::app);
            if (!file) {
                std::cerr << "failed to create log file: " << filePath << std::endl;
                continue;
            }

            while(!logs.empty()) {
                file << '[' << timeBuf << "] "
                     << logs.front() << '\n';
                logs.pop();
            }

            file.close();
        }
    });
    // 分离线程
    writeLogTask.detach();
}

Logger::~Logger()
{
    // 同步 flush 队列中剩余的日志（caller 快速退出时确保日志不丢失）
    std::time_t now = time(nullptr);
    std::tm now_tm;
    localtime_r(&now, &now_tm);

    std::string procName = getProcName();
    char filePath[160];
    snprintf(filePath, sizeof(filePath),
             "logs/%d-%d-%d_%s.log",
             now_tm.tm_year + 1900,
             now_tm.tm_mon + 1,
             now_tm.tm_mday,
             procName.c_str());

    char timeBuf[16];
    snprintf(timeBuf, sizeof(timeBuf), "%02d:%02d:%02d",
             now_tm.tm_hour, now_tm.tm_min, now_tm.tm_sec);

    std::queue<std::string> remaining;
    m_lockQue.drain(remaining);

    if (!remaining.empty()) {
        std::ofstream file(filePath, std::ios::app);
        if (file) {
            while (!remaining.empty()) {
                file << '[' << timeBuf << "] "
                     << remaining.front() << std::endl;
                remaining.pop();
            }
        }
    }
}

Logger& Logger::getInstance()
{
    static Logger logger;
    return logger;
}

// 外部接口-把日志写入lockQueue缓冲区中
void Logger::log(std::string msg)
{
    m_lockQue.push(msg);
}
