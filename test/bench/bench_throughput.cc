// 指标5：吞吐量（逐块往返 vs 批量往返）
// 本地回环 TCP，复用同一条连接，对比「N 次小往返」与「1 次大往返」的吞吐，
// 验证批量聚合把 RPC 往返从 N 降到 1 带来的吞吐提升。
#include <iostream>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

static const int PORT = 18002;
static const int CHUNK_SIZE = 1024;
static const int N = 4096;   // 块数（总 4MB）

static int tcpConnect(const char* ip, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static void echoLoop(int listenFd, std::atomic<bool>& stop)
{
    while (!stop.load()) {
        int fd = accept(listenFd, nullptr, nullptr);
        if (fd < 0) break;
        char buf[8192];
        for (;;) {
            ssize_t r = recv(fd, buf, sizeof(buf), 0);
            if (r <= 0) break;
            ssize_t off = 0;
            bool ok = true;
            while (off < r) {
                ssize_t s = send(fd, buf + off, r - off, 0);
                if (s <= 0) { ok = false; break; }
                off += s;
            }
            if (!ok) break;
        }
        close(fd);
    }
}

static bool sendAll(int fd, const char* data, int size)
{
    int off = 0;
    while (off < size) {
        ssize_t s = send(fd, data + off, size - off, 0);
        if (s <= 0) return false;
        off += s;
    }
    return true;
}

static bool recvAll(int fd, char* data, int size)
{
    int got = 0;
    while (got < size) {
        ssize_t r = recv(fd, data + got, size - got, 0);
        if (r <= 0) return false;
        got += r;
    }
    return true;
}

int main()
{
    int listenFd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);
    int yes = 1;
    setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    if (bind(listenFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        listen(listenFd, 16) != 0) {
        std::cerr << "bind/listen failed (port " << PORT << " occupied?)" << std::endl;
        return 1;
    }

    std::atomic<bool> stop(false);
    std::thread server(echoLoop, listenFd, std::ref(stop));

    std::string chunk(CHUNK_SIZE, 'y');
    const long long totalBytes = static_cast<long long>(N) * CHUNK_SIZE;

    std::cout << "===== 指标5：吞吐量（逐块往返 vs 批量往返） =====" << std::endl;
    std::cout << "块数 N = " << N << "，总字节 = " << (totalBytes / 1024 / 1024) << " MB" << std::endl;

    // 模式 A：N 次小往返（优化前逐块 RPC）
    {
        int fd = tcpConnect("127.0.0.1", PORT);
        char buf[CHUNK_SIZE];
        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < N; ++i) {
            sendAll(fd, chunk.data(), CHUNK_SIZE);
            recvAll(fd, buf, CHUNK_SIZE);
        }
        auto end = std::chrono::steady_clock::now();
        close(fd);
        double sec = std::chrono::duration<double>(end - start).count();
        double mbps = totalBytes / sec / 1024.0 / 1024.0;
        std::cout << "[优化前 逐块往返] 耗时 = " << sec << " s，吞吐 = "
                  << mbps << " MB/s" << std::endl;
    }

    // 模式 B：1 次大批量往返（优化后批量 RPC）
    {
        int fd = tcpConnect("127.0.0.1", PORT);
        std::string big(static_cast<size_t>(totalBytes), 'z');
        std::string out(static_cast<size_t>(totalBytes), '\0');
        auto start = std::chrono::steady_clock::now();
        sendAll(fd, big.data(), static_cast<int>(big.size()));
        recvAll(fd, &out[0], static_cast<int>(out.size()));
        auto end = std::chrono::steady_clock::now();
        close(fd);
        double sec = std::chrono::duration<double>(end - start).count();
        double mbps = totalBytes / sec / 1024.0 / 1024.0;
        std::cout << "[优化后 批量往返] 耗时 = " << sec << " s，吞吐 = "
                  << mbps << " MB/s" << std::endl;
    }

    stop.store(true);
    shutdown(listenFd, SHUT_RDWR);
    close(listenFd);
    server.join();
    return 0;
}
