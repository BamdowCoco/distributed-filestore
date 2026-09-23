// 指标1：单块 MD5 校验耗时
// 循环对 1024 字节数据调用 md5Hex，取平均耗时，验证简历「单块校验耗时」的数量级。
#include <iostream>
#include <string>
#include <chrono>

#include "common.h"   // md5Hex

// 单元块大小：与框架 CHUNK_SIZE 解耦，保持 1KB 以便快速测量 per-byte 成本
constexpr int BLOCK_SIZE = 1024;

int main()
{
    // 构造一块 1024 字节的测试数据（模拟文件块）
    std::string data(BLOCK_SIZE, '\0');
    for (int i = 0; i < BLOCK_SIZE; ++i) {
        data[i] = static_cast<char>(i * 31 + 7);
    }

    const int N = 200000;

    // 预热：排除首次调用的冷启动 / 分支预测影响
    std::string sink;
    for (int i = 0; i < 2000; ++i) {
        sink = md5Hex(data);
    }

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < N; ++i) {
        sink = md5Hex(data);
    }
    auto end = std::chrono::steady_clock::now();

    double totalMs = std::chrono::duration<double, std::milli>(end - start).count();
    double perUs = totalMs * 1000.0 / N;

    std::cout << "===== 指标1：单块 MD5 校验耗时 =====" << std::endl;
    std::cout << "块大小 BLOCK_SIZE = " << BLOCK_SIZE << " 字节" << std::endl;
    std::cout << "循环次数 N = " << N << std::endl;
    std::cout << "总耗时 = " << totalMs << " ms" << std::endl;
    std::cout << "平均每次 md5Hex = " << perUs << " us" << std::endl;
    std::cout << "示例 MD5 = " << sink << std::endl;
    std::cout << std::endl;
    std::cout << "说明：纯 md5Hex 计算在微秒级；简历「约 5ms」是含 TCP 往返 + 落盘的端到端单块耗时，" << std::endl;
    std::cout << "      二者口径不同，本测试仅给出校验计算本身的开销上限。" << std::endl;
    return 0;
}
