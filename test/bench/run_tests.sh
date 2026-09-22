#!/usr/bin/env bash
# 一键编译并运行 5 个本地基准测试（不依赖 ZooKeeper / MySQL / 服务进程）
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

echo "[1/5] bench_md5"
$CXX $CXXFLAGS $COMMON_INC "$ROOT/test/bench/bench_md5.cc" -o "$BUILD_DIR/bench_md5" -lcrypto

echo "[2/5] test_consistent_hash"
$CXX $CXXFLAGS $COMMON_INC "$ROOT/test/bench/test_consistent_hash.cc" -o "$BUILD_DIR/test_consistent_hash" -lcrypto

echo "[3/5] bench_connect"
$CXX $CXXFLAGS "$ROOT/test/bench/bench_connect.cc" -o "$BUILD_DIR/bench_connect" -pthread

echo "[4/5] test_compact_storage"
$CXX $CXXFLAGS $COMMON_INC "$ROOT/test/bench/test_compact_storage.cc" -o "$BUILD_DIR/test_compact_storage" -lcrypto

echo "[5/5] bench_throughput"
$CXX $CXXFLAGS "$ROOT/test/bench/bench_throughput.cc" -o "$BUILD_DIR/bench_throughput" -pthread

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
echo "========== 本地基准全部完成 =========="
