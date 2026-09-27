#!/usr/bin/env bash
# 一键编译并运行 7 个本地测试（不依赖 ZooKeeper / MySQL / 服务进程）
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

echo "[1/7] bench_md5"
$CXX $CXXFLAGS $COMMON_INC "$ROOT/test/bench/bench_md5.cc" -o "$BUILD_DIR/bench_md5" -lcrypto

echo "[2/7] test_consistent_hash"
$CXX $CXXFLAGS $COMMON_INC "$ROOT/test/bench/test_consistent_hash.cc" -o "$BUILD_DIR/test_consistent_hash" -lcrypto

echo "[3/7] bench_connect"
$CXX $CXXFLAGS "$ROOT/test/bench/bench_connect.cc" -o "$BUILD_DIR/bench_connect" -pthread

echo "[4/7] test_compact_storage"
$CXX $CXXFLAGS $COMMON_INC "$ROOT/test/bench/test_compact_storage.cc" -o "$BUILD_DIR/test_compact_storage" -lcrypto

echo "[5/7] bench_throughput"
$CXX $CXXFLAGS "$ROOT/test/bench/bench_throughput.cc" -o "$BUILD_DIR/bench_throughput" -pthread

echo "[6/7] test_ticket"
$CXX $CXXFLAGS "$ROOT/test/bench/test_ticket.cc" -o "$BUILD_DIR/test_ticket" -lcrypto

echo "[7/7] test_thread_pool"
$CXX $CXXFLAGS -I"$ROOT/include" "$ROOT/test/bench/test_thread_pool.cc" -o "$BUILD_DIR/test_thread_pool" -lpthread

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
echo "========== 本地基准全部完成 =========="
