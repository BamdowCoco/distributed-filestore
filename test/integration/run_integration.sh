#!/usr/bin/env bash
# 集成测试：启动 meta/storage 服务，用 strace/time 测真实上传的连接开销与吞吐量。
# 前置：ZooKeeper 已在 2181 监听、MySQL 已在 3306 监听、bin/ 下已构建出可执行文件。
set -e

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

BIN="$ROOT/bin"
CONFIG="$ROOT/config"
TEST_DIR="$ROOT/test/integration"
LOG_DIR="$TEST_DIR/log"

mkdir -p "$LOG_DIR"

echo "========== 前置检查 =========="
if ! ss -ltn 2>/dev/null | grep -q ':2181'; then
    echo "[错误] ZooKeeper 未在 2181 监听，请先启动 zkServer.sh"
    exit 1
fi
if ! ss -ltn 2>/dev/null | grep -q ':3306'; then
    echo "[错误] MySQL 未在 3306 监听"
    exit 1
fi
for b in meta_callee storage_callee fs_caller; do
    if [ ! -x "$BIN/$b" ]; then
        echo "[错误] $BIN/$b 不存在，请先在项目根执行 ./autobuild.sh"
        exit 1
    fi
done
echo "ZooKeeper / MySQL / 可执行文件 均就绪"

PIDS=""
cleanup() {
    echo
    echo "========== 清理 =========="
    # 先删元数据（需服务仍在运行），再停服务、清落盘
    "$BIN/fs_caller" -i "$CONFIG/filestore_meta.cnf" delete strace.bin >/dev/null 2>&1 || true
    "$BIN/fs_caller" -i "$CONFIG/filestore_meta.cnf" delete time.bin >/dev/null 2>&1 || true
    if [ -n "$PIDS" ]; then
        kill $PIDS 2>/dev/null || true
    fi
    rm -rf "$ROOT/filestore/data_node1" "$ROOT/filestore/data_node2"
    rm -f "$TEST_DIR"/*.bin
}
trap cleanup EXIT

echo
echo "========== 启动服务 =========="
"$BIN/storage_callee" -i "$CONFIG/filestore_storage1.cnf" > "$LOG_DIR/storage1.log" 2>&1 &
PIDS="$!"
"$BIN/storage_callee" -i "$CONFIG/filestore_storage2.cnf" > "$LOG_DIR/storage2.log" 2>&1 &
PIDS="$PIDS $!"
sleep 1
"$BIN/meta_callee" -i "$CONFIG/filestore_meta.cnf" > "$LOG_DIR/meta.log" 2>&1 &
PIDS="$PIDS $!"

echo "等待服务端口就绪..."
READY=0
for i in $(seq 1 40); do
    READY=0
    for p in 8001 8002 8003; do
        ss -ltn 2>/dev/null | grep -q ":$p " && READY=$((READY + 1))
    done
    [ "$READY" -eq 3 ] && break
    sleep 0.5
done
if [ "$READY" -ne 3 ]; then
    echo "[错误] 服务端口未就绪，日志见 $LOG_DIR/"
    exit 1
fi
echo "8001 / 8002 / 8003 已就绪"

# 生成 1MB 随机内容，复制成两个不同文件名（避免第二次上传「文件已存在」）
dd if=/dev/urandom of="$TEST_DIR/blob.bin" bs=1024 count=1024 2>/dev/null
cp "$TEST_DIR/blob.bin" "$TEST_DIR/strace.bin"
cp "$TEST_DIR/blob.bin" "$TEST_DIR/time.bin"
FSIZE=$(stat -c%s "$TEST_DIR/blob.bin")
CHUNKS=$(( (FSIZE + 1023) / 1024 ))
echo "测试文件：$FSIZE 字节（$CHUNKS 块，CHUNK_SIZE=1024）"

echo
echo "========== 连接开销（strace 统计 connect） =========="
if command -v strace >/dev/null 2>&1; then
    STRACE_OUT="$LOG_DIR/strace_connect.out"
    strace -e trace=connect -o "$STRACE_OUT" \
        "$BIN/fs_caller" -i "$CONFIG/filestore_meta.cnf" upload "$TEST_DIR/strace.bin" \
        > "$LOG_DIR/upload_strace.out" 2>&1 || true

    # 统计连到存储节点(8001/8002)的 connect 次数（长连接复用，每节点 1 次）
    STORAGE_CONNECTS=$(grep -cE 'sin_port=htons\((8001|8002)\)' "$STRACE_OUT" || true)
    STORAGE_CONNECTS=${STORAGE_CONNECTS:-0}

    echo "优化后：连到存储节点的 connect 次数 = $STORAGE_CONNECTS（长连接复用，每节点 1 次）"
    echo "优化前（理论）：每块新建连接 = $CHUNKS 次"
    awk -v a="$CHUNKS" -v b="$STORAGE_CONNECTS" \
        'BEGIN { if (a > 0) printf "连接开销降低 = %.1f%%（%d -> %d）\n", 100.0*(a-b)/a, a, b }'
else
    echo "[跳过] 未安装 strace，请 apt install strace 后重跑"
fi

echo
echo "========== 吞吐量（time 计时真实上传） =========="
START=$(date +%s.%N)
"$BIN/fs_caller" -i "$CONFIG/filestore_meta.cnf" upload "$TEST_DIR/time.bin" \
    > "$LOG_DIR/upload_time.out" 2>&1 || {
    echo "[错误] 上传失败，日志见 $LOG_DIR/upload_time.out"
    exit 1
}
END=$(date +%s.%N)
ELAPSED=$(awk -v s="$START" -v e="$END" 'BEGIN { printf "%.4f", e - s }')
echo "上传耗时 = ${ELAPSED} s"
awk -v b="$FSIZE" -v t="$ELAPSED" \
    'BEGIN { printf "吞吐 = %.2f MB/s\n", b/t/1024/1024 }'

echo
echo "========== 集成测试完成 =========="
