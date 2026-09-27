// 生成 Ed25519 密钥对（存储访问票据的签发密钥）。
//
// 用法：ticket_keygen [输出目录]      默认写 ./keys/
//   keys/ticket_priv.pem  0600  —— 只放在元数据服务那一台机器上，**不要**提交、不要外传
//   keys/ticket_pub.pem   0644  —— 公钥不是机密，可分发到各存储节点（也可直接入库）
// 同时打印 kid（= SHA256(SPKI DER) 前 32 个十六进制字符）。
//
// 为什么要有个工具、而不是让运维敲 openssl：kid 必须与元数据/存储节点算出的完全一致，
// 工具里复用同一份 ticket::TicketKey 实现，就不会出现两边算法漂移。
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <string>

#include <openssl/evp.h>
#include <openssl/pem.h>

#include "common/ticket.h"

int main(int argc, char** argv)
{
    const std::string dir = (argc > 1) ? argv[1] : "keys";
    if (mkdir(dir.c_str(), 0700) != 0) {
        // 目录已存在是正常的；其它错误后面写文件时会暴露
    }
    const std::string privPath = dir + "/ticket_priv.pem";
    const std::string pubPath = dir + "/ticket_pub.pem";

    EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, nullptr);
    if (ctx == nullptr) {
        std::fprintf(stderr, "EVP_PKEY_CTX_new_id(ED25519) failed\n");
        return 1;
    }
    EVP_PKEY* key = nullptr;
    if (EVP_PKEY_keygen_init(ctx) != 1 || EVP_PKEY_keygen(ctx, &key) != 1) {
        std::fprintf(stderr, "EVP_PKEY_keygen failed\n");
        EVP_PKEY_CTX_free(ctx);
        return 1;
    }
    EVP_PKEY_CTX_free(ctx);

    // 私钥：以 0600 创建（先 open 再 fdopen，避免"先按 umask 建好再 chmod"之间
    // 存在一段权限过宽的窗口）
    int fd = open(privPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        std::fprintf(stderr, "cannot create %s\n", privPath.c_str());
        EVP_PKEY_free(key);
        return 1;
    }
    FILE* fp = fdopen(fd, "w");
    if (fp == nullptr || PEM_write_PrivateKey(fp, key, nullptr, nullptr, 0, nullptr, nullptr) != 1) {
        std::fprintf(stderr, "failed to write %s\n", privPath.c_str());
        if (fp != nullptr) {
            fclose(fp);
        } else {
            close(fd);
        }
        EVP_PKEY_free(key);
        return 1;
    }
    fclose(fp);

    fp = fopen(pubPath.c_str(), "w");
    if (fp == nullptr || PEM_write_PUBKEY(fp, key) != 1) {
        std::fprintf(stderr, "failed to write %s\n", pubPath.c_str());
        if (fp != nullptr) {
            fclose(fp);
        }
        EVP_PKEY_free(key);
        return 1;
    }
    fclose(fp);

    // 交给 TicketKey 托管（顺便用同一份实现算 kid），析构时自动释放
    ticket::TicketKey holder;
    if (!holder.adopt(key)) {
        std::fprintf(stderr, "generated key is not Ed25519?!\n");
        return 1;
    }
    const std::string kid = holder.kid();

    std::printf("私钥: %s  (0600，只放在元数据服务那台机器上，勿提交)\n", privPath.c_str());
    std::printf("公钥: %s  (0644，可分发到各存储节点)\n", pubPath.c_str());
    std::printf("  kid: %s\n\n", kid.c_str());
    std::printf("元数据服务配置： [auth] ticket_privkey=%s\n", privPath.c_str());
    std::printf("存储节点配置：   [auth] ticket_pubkey=%s\n", pubPath.c_str());
    return 0;
}
