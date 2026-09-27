#!/usr/bin/env bash
# 一键编译并运行 8 个本地测试（不依赖 ZooKeeper / MySQL / 服务进程）
set -e

# 定位项目根目录（脚本在 test/bench/ 下）
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

BUILD_DIR="$ROOT/test/bench/build"
mkdir -p "$BUILD_DIR"

CXX="${CXX:-g++}"
CXXFLAGS="-O2 -std=c++11"
COMMON_INC="-I$ROOT/filestore/common"

echo "========== 编译本地基准 =========="

echo "[1/8] bench_md5"
$CXX $CXXFLAGS $COMMON_INC "$ROOT/test/bench/bench_md5.cc" -o "$BUILD_DIR/bench_md5" -lcrypto

echo "[2/8] test_consistent_hash"
$CXX $CXXFLAGS $COMMON_INC "$ROOT/test/bench/test_consistent_hash.cc" -o "$BUILD_DIR/test_consistent_hash" -lcrypto

echo "[3/8] bench_connect"
$CXX $CXXFLAGS "$ROOT/test/bench/bench_connect.cc" -o "$BUILD_DIR/bench_connect" -pthread

echo "[4/8] test_compact_storage"
$CXX $CXXFLAGS $COMMON_INC "$ROOT/test/bench/test_compact_storage.cc" -o "$BUILD_DIR/test_compact_storage" -lcrypto

echo "[5/8] bench_throughput"
$CXX $CXXFLAGS "$ROOT/test/bench/bench_throughput.cc" -o "$BUILD_DIR/bench_throughput" -pthread

echo "[6/8] test_ticket"
$CXX $CXXFLAGS "$ROOT/test/bench/test_ticket.cc" -o "$BUILD_DIR/test_ticket" -lcrypto

echo "[7/8] test_thread_pool"
$CXX $CXXFLAGS -I"$ROOT/include" "$ROOT/test/bench/test_thread_pool.cc" -o "$BUILD_DIR/test_thread_pool" -lpthread

echo "[8/8] test_password"
$CXX $CXXFLAGS $COMMON_INC "$ROOT/test/bench/test_password.cc" -o "$BUILD_DIR/test_password" -lcrypto

echo
echo "========== 运行本地基准 =========="
echo

echo ">> bench_md5（单块校验耗时）"
"$BUILD_DIR/bench_md5"

echo
echo ">> test_consistent_hash（块重映射）"
"$BUILD_DIR/test_consistent_hash"

echo
echo ">> bench_connect（连接开销）"
"$BUILD_DIR/bench_connect"

echo
echo ">> test_compact_storage（空间利用率）"
"$BUILD_DIR/test_compact_storage"

echo
echo ">> bench_throughput（吞吐量）"
"$BUILD_DIR/bench_throughput"

echo
echo ">> test_ticket（票据验签 + 不可信数字解析边界）"
"$BUILD_DIR/test_ticket"

echo
echo ">> test_thread_pool（工作线程池：任务执行 / 停止不挂死 / 背压）"
"$BUILD_DIR/test_thread_pool"

echo
echo ">> test_password（口令散列：加盐/校验/旧格式兼容/畸形输入）"
"$BUILD_DIR/test_password"

echo
echo "========== 本地基准全部完成 =========="
