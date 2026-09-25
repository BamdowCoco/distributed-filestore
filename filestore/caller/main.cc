#include <iostream>
#include <string>
#include <unistd.h>

#include "fs_client.h"
#include "mprpc_application.h"

static void usage(const char* prog)
{
    std::cerr << "usage: " << prog << " -i <configfile> <command> [args]\n"
              << "commands:\n"
              << "  register <user> <pwd>         注册\n"
              << "  login <user> <pwd>            登录\n"
              << "  logout                        退出登录\n"
              << "  ls [path]                     列出目录\n"
              << "  mkdir <path>                  建目录\n"
              << "  rmdir [-r] <path>             删目录（-r 递归删除子目录/文件）\n"
              << "  cd <path>                     切换当前目录（本地）\n"
              << "  upload <本地文件> <虚拟路径>  上传\n"
              << "  download <虚拟路径> [-o 目标] 下载（默认 ~/Downloads）\n"
              << "  delete <虚拟路径>             删除\n";
}

int main(int argc, char** argv)
{
    MprpcApplication::init(argc, argv);

    if (optind >= argc) {
        usage(argv[0]);
        return 1;
    }
    std::string cmd = argv[optind];

    FsClient client;

    if (cmd == "register") {
        if (optind + 2 >= argc) { usage(argv[0]); return 1; }
        if (!client.registerUser(argv[optind + 1], argv[optind + 2])) {
            return 1;
        }
        std::cout << "register success" << std::endl;
        return 0;
    } else if (cmd == "login") {
        if (optind + 2 >= argc) { usage(argv[0]); return 1; }
        if (client.login(argv[optind + 1], argv[optind + 2])) {
            std::cout << "login success, token saved" << std::endl;
            return 0;
        }
        return 1;
    } else if (cmd == "logout") {
        if (!client.logout()) {
            return 1;
        }
        std::cout << "logout done" << std::endl;
        return 0;
    } else if (cmd == "ls") {
        std::string path = (optind + 1 < argc) ? argv[optind + 1] : "";
        return client.listDir(path) ? 0 : 1;
    } else if (cmd == "mkdir") {
        if (optind + 1 >= argc) { usage(argv[0]); return 1; }
        if (!client.mkdir(argv[optind + 1])) {
            return 1;
        }
        std::cout << "mkdir " << argv[optind + 1] << " done" << std::endl;
        return 0;
    } else if (cmd == "rmdir") {
        bool recursive = false;
        int pathIdx = optind + 1;
        if (pathIdx < argc && std::string(argv[pathIdx]) == "-r") {
            recursive = true;
            ++pathIdx;
        }
        if (pathIdx >= argc) { usage(argv[0]); return 1; }
        if (!client.rmdir(argv[pathIdx], recursive)) {
            return 1;
        }
        std::cout << "rmdir " << argv[pathIdx] << (recursive ? " -r" : "") << " done" << std::endl;
        return 0;
    } else if (cmd == "cd") {
        if (optind + 1 >= argc) { usage(argv[0]); return 1; }
        if (!client.changeDir(argv[optind + 1])) {
            return 1;
        }
        std::cout << "cwd -> " << argv[optind + 1] << std::endl;
        return 0;
    } else if (cmd == "upload") {
        if (optind + 2 >= argc) { usage(argv[0]); return 1; }
        return client.upload(argv[optind + 1], argv[optind + 2]) ? 0 : 1;
    } else if (cmd == "download") {
        if (optind + 1 >= argc) { usage(argv[0]); return 1; }
        std::string dest;
        for (int i = optind + 2; i < argc; ++i) {
            if (std::string(argv[i]) == "-o" && i + 1 < argc) {
                dest = argv[i + 1];
                break;
            }
        }
        return client.download(argv[optind + 1], dest) ? 0 : 1;
    } else if (cmd == "delete") {
        if (optind + 1 >= argc) { usage(argv[0]); return 1; }
        return client.remove(argv[optind + 1]) ? 0 : 1;
    } else {
        usage(argv[0]);
        return 1;
    }
}
