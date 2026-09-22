// 指标3：连接开销（长连接复用 vs 每块新建连接）
// 本地回环 TCP，对比「每消息新建连接」与「复用一条连接」的 connect 次数与总耗时。
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

static const int PORT = 18001;
static const int MSG_SIZE = 1024;
static const int M = 500;   // 消息 / 连接次数

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

// 串行回显服务：accept 一个连接后循环 recv/send，直到对端关闭
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

// 一次往返：发满 size 字节 + 收满 size 字节
static bool roundTrip(int fd, const char* data, int size)
{
    int off = 0;
    while (off < size) {
        ssize_t s = send(fd, data + off, size - off, 0);
        if (s <= 0) return false;
        off += s;
    }
    char buf[MSG_SIZE];
    int got = 0;
    while (got < size) {
        ssize_t r = recv(fd, buf + got, size - got, 0);
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

    std::string msg(MSG_SIZE, 'x');

    std::cout << "===== 指标3：连接开销（复用 vs 新建） =====" << std::endl;
    std::cout << "消息数 M = " << M << "（模拟 M 个块）" << std::endl;

    // 模式 A：每消息新建连接（优化前）
    {
        auto start = std::chrono::steady_clock::now();
        int connects = 0;
        for (int i = 0; i < M; ++i) {
            int fd = tcpConnect("127.0.0.1", PORT);
            if (fd < 0) break;
            ++connects;
            roundTrip(fd, msg.data(), MSG_SIZE);
            close(fd);
        }
        auto end = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "[优化前 每块新建连接] connect 次数 = " << connects
                  << "，总耗时 = " << ms << " ms" << std::endl;
    }

    // 模式 B：复用一条连接（优化后）
    {
        auto start = std::chrono::steady_clock::now();
        int connects = 0;
        int fd = tcpConnect("127.0.0.1", PORT);
        if (fd >= 0) {
            ++connects;
            for (int i = 0; i < M; ++i) {
                roundTrip(fd, msg.data(), MSG_SIZE);
            }
            close(fd);
        }
        auto end = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "[优化后 长连接复用]   connect 次数 = " << connects
                  << "，总耗时 = " << ms << " ms" << std::endl;
        std::cout << "connect 次数降低 = " << (100.0 * (M - 1) / M)
                  << "%（" << M << " -> 1）" << std::endl;
    }

    stop.store(true);
    shutdown(listenFd, SHUT_RDWR);
    close(listenFd);
    server.join();
    return 0;
}
