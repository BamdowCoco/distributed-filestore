#!/usr/bin/env bash
# 统一执行「未做过」的新测试，每个测试结束后还原状态再做下一个，最后生成测试报告。
#
# 包含 4 个测试：
#   1. RPC 调用延迟（直连 FriendService 循环测往返延迟）
#   2. 10 节点动态发现（短暂验证，不上传）
#   3. 循环上传极限（3 节点降载，1MB 文件次数循序渐进，只取最大成功规模）
#   4. 大文件极限（3 节点降载，64MB→5GB 循序渐进，只取最大成功大小 + 下载 md5 一致）
#
# 前置：ZooKeeper 已监听 2181、MySQL 已监听 3306、已 ./autobuild.sh 构建出 bin/ 产物。
#       2GB VM 建议用小堆启动 ZK：SERVER_JVMFLAGS="-Xms128m -Xmx256m" zkServer.sh start
# 用法：bash test/integration/run_new_tests.sh

# ---- 可调参数（可用环境变量覆盖，冒烟测试传小序列） ----
UPLOAD_SEQ=(${UPLOAD_SEQ_OVERRIDE:-100 500 1000 2000 5000 10000})   # 循环上传：次数渐进序列
BIGFILE_SEQ_MB=(${BIGFILE_SEQ_MB_OVERRIDE:-64 256 1024 2048 5120})   # 大文件：大小渐进序列（MB）
DISCOVERY_PORTS=(8001 8002 8004 8005 8006 8007 8008 8009 8010 8011)  # 10 节点动态发现（短暂验证，不上传）
SCALE_PORTS=(8001 8002 8004)                                          # 规模测试 3 节点（降载，适配 2GB VM）
RPC_LATENCY_N=10000                             # RPC 延迟采样次数
CLUSTER_DISCOVER_WAIT=12                        # 等 meta 发现全部节点的秒数
TIMEOUT_UPLOAD=600                              # 单次 fs_caller 超时（秒）

# ---- 路径 ----
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
BIN="$ROOT/bin"
CONFIG="$ROOT/config"
TMP_DIR="$ROOT/test/integration/tmp"
LOG_DIR="$ROOT/test/integration/log"
REPORT="$ROOT/docs/filestore/test/test-report.md"
CALLEE_LOG="$LOG_DIR/callee.log"
META_LOG="$LOG_DIR/meta.log"

mkdir -p "$TMP_DIR/cfg" "$LOG_DIR"

# ---- 结果收集（写进报告）----
RPC_RESULT=""
UPLOAD_MAX_N=""
UPLOAD_AVG_MS=""
DISCOVER_COUNT=""
BIGFILE_MAX_MB=""
BIGFILE_THROUGHPUT=""
BIGFILE_MD5=""

# ============ 前置检查 ============
echo "========== 前置检查 =========="
if ! ss -ltn 2>/dev/null | grep -q ':2181'; then
    echo "[错误] ZooKeeper 未在 2181 监听，请先启动 zkServer.sh"
    exit 1
fi
if ! ss -ltn 2>/dev/null | grep -q ':3306'; then
    echo "[错误] MySQL 未在 3306 监听"
    exit 1
fi
for b in callee bench_rpc_latency storage_callee meta_callee fs_caller; do
    if [ ! -x "$BIN/$b" ]; then
        echo "[错误] $BIN/$b 不存在，请先在项目根执行 ./autobuild.sh"
        exit 1
    fi
done
command -v mysql >/dev/null 2>&1 || { echo "[错误] 缺少 mysql CLI"; exit 1; }
echo "ZooKeeper / MySQL / 可执行文件 均就绪"

# ============ 工具函数 ============

# 从 config/mysql.cnf 解析 [database] 段
mysql_ip=""; mysql_port=""; mysql_user=""; mysql_pass=""; mysql_db=""
parse_mysql_conf() {
    local sec=""
    while IFS= read -r line; do
        case "$line" in
            \[*\]) sec="${line#\[}"; sec="${sec%\]}";;
            ip=*)   [ "$sec" = "database" ] && mysql_ip="${line#ip=}";;
            port=*) [ "$sec" = "database" ] && mysql_port="${line#port=}";;
            username=*) [ "$sec" = "database" ] && mysql_user="${line#username=}";;
            password=*) [ "$sec" = "database" ] && mysql_pass="${line#password=}";;
            dbname=*) [ "$sec" = "database" ] && mysql_db="${line#dbname=}";;
        esac
    done < "$CONFIG/mysql.cnf"
}

# 清空 MySQL 元数据表（先子后父）
clear_mysql() {
    parse_mysql_conf
    [ -n "$mysql_db" ] || { echo "[错误] 无法解析 config/mysql.cnf"; exit 1; }
    mysql -h"$mysql_ip" -P"$mysql_port" -u"$mysql_user" -p"$mysql_pass" "$mysql_db" \
        -e "DELETE FROM file_chunk; DELETE FROM file_meta;" >/dev/null 2>&1 \
        || echo "[警告] 清空 MySQL 失败（可能表尚未建立，忽略）"
}

# 档间清理：清 MySQL 元数据 + 清各 storage 落盘文件（不杀进程）。
# 否则上一档的同名 remoteName 会让 UploadFile 查重失败，且落盘文件会被 fseek(SEEK_END) 追加污染。
clear_cluster_data() {
    clear_mysql
    rm -f "$TMP_DIR"/data_node*/* 2>/dev/null
}

# 还原：杀本轮进程 + 清 MySQL + 删落盘/下载/临时
PIDS=""
restore() {
    echo
    echo "---------- 还原状态 ----------"
    if [ -n "$PIDS" ]; then
        kill $PIDS 2>/dev/null || true
        PIDS=""
    fi
    # 等 ZK 临时节点随会话超时（zookeeper_init 的 30s timeout）删除。
    # 否则下一个测试用相同 ip:port 起新节点时，旧临时节点仍被旧 session 持有，
    # 新节点 zoo_create 返回 ZNODEEXISTS（被竞态容忍逻辑吞掉），实际未注册成功，
    # meta 轮询到空结果只能保留初始环，导致「10 节点」退化为 storage_nodes 里的 2 节点。
    echo "等待 ZK 临时节点过期（约 35s）..."
    sleep 35
    clear_mysql
    rm -rf "$TMP_DIR/cfg" "$TMP_DIR"/data_node* "$TMP_DIR"/up_*.bin "$TMP_DIR"/big.bin \
           downloads
    mkdir -p "$TMP_DIR/cfg"
    echo "已还原（进程 / MySQL / 落盘 / 临时文件）"
}

# 等一组端口全部监听
wait_ports() {
    local ports="$1" max_wait="$2"
    local i p
    for i in $(seq 1 "$max_wait"); do
        local ready=0
        for p in $ports; do
            ss -ltn 2>/dev/null | grep -q ":$p " && ready=$((ready + 1))
        done
        [ "$ready" -eq "$(echo "$ports" | wc -w)" ] && return 0
        sleep 0.5
    done
    return 1
}

# 内存守卫：2GB VM 上跑多进程易 OOM 拖死 ZooKeeper，起集群前检查可用内存
check_mem() {
    local avail_kb
    avail_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo 2>/dev/null)
    [ -z "$avail_kb" ] && return 0
    if [ "$avail_kb" -lt $((256 * 1024)) ]; then
        echo "[错误] 可用内存仅 $((avail_kb / 1024))MB（<256MB），2GB VM 内存不足。请先关闭编辑器/语言服务，或用小堆启动 ZooKeeper，再重跑。" >&2
        exit 1
    elif [ "$avail_kb" -lt $((512 * 1024)) ]; then
        echo "[警告] 可用内存仅 $((avail_kb / 1024))MB（<512MB），测试可能因内存压力导致 ZooKeeper 崩溃。" >&2
    fi
}

# ZooKeeper 存活守卫：返回 0 表示 ZK 仍在 2181 监听
zk_alive() {
    ss -ltn 2>/dev/null | grep -q ':2181'
}

# 起 N 个 storage + 1 个 meta，等待动态发现；通过全局 CLUSTER_COUNT 返回实际发现节点数（0 表示失败）。
# 参数：$1 = 空格分隔的端口列表；$2 = 期望节点数（仅日志）；$3 = 标签（仅日志）
start_cluster() {
    local -a ports=($1)
    local expected="$2" label="$3"
    local i
    for i in "${!ports[@]}"; do
        local port="${ports[$i]}"
        sed -e "s/^rpc_server_port=.*/rpc_server_port=$port/" \
            -e "s#^data_dir=.*#data_dir=$TMP_DIR/data_node$((i+1))#" \
            "$CONFIG/filestore_storage3.cnf" > "$TMP_DIR/cfg/storage$((i+1)).cnf"
        "$BIN/storage_callee" -i "$TMP_DIR/cfg/storage$((i+1)).cnf" > "$LOG_DIR/storage$((i+1)).log" 2>&1 &
        PIDS="$PIDS $!"
    done
    sleep 1
    "$BIN/meta_callee" -i "$CONFIG/filestore_meta.cnf" > "$META_LOG" 2>&1 &
    PIDS="$PIDS $!"

    local all_ports="$1 8003"   # 存储节点 + meta(8003)
    if ! wait_ports "$all_ports" 80; then
        echo "[错误] 服务端口未全部就绪，日志见 $LOG_DIR/" >&2
        CLUSTER_COUNT=0
        return
    fi
    echo "${label:-集群}：${#ports[@]} 个存储节点 + meta 端口已就绪" >&2

    # 等 meta 后台线程（每 3s 轮询 ZK）发现全部节点
    echo "等待 meta 动态发现存储节点（约 ${CLUSTER_DISCOVER_WAIT}s）..." >&2
    sleep "$CLUSTER_DISCOVER_WAIT"
    local count
    count=$(grep -oE 'refreshed storage nodes, count:[0-9]+' "$META_LOG" | tail -1 | grep -oE '[0-9]+$')
    echo "meta 最近一次发现节点数 = ${count:-未检测到}（期望 ${expected}）" >&2
    CLUSTER_COUNT="${count:-0}"
}

# ============ 测试 1：RPC 调用延迟 ============
echo
echo "========== 测试 1：RPC 调用延迟 =========="
"$BIN/callee" -i "$CONFIG/mprpc.cnf" > "$CALLEE_LOG" 2>&1 &
PIDS="$!"
if ! wait_ports "8000" 20; then
    echo "[错误] callee(8000) 未就绪"
    restore
    exit 1
fi
echo "callee(FriendService, 8000) 已就绪，循环 $RPC_LATENCY_N 次测延迟..."
RPC_RESULT="$("$BIN/bench_rpc_latency" "$RPC_LATENCY_N" 2>&1)"
echo "$RPC_RESULT"
# 从 RPC_LATENCY 输出提取 avg/median/p99，供报告与 resume 回填使用
RPC_AVG_US=$(echo "$RPC_RESULT" | sed -nE 's/.*avg=([0-9.]+)us.*/\1/p')
RPC_MEDIAN_US=$(echo "$RPC_RESULT" | sed -nE 's/.*median=([0-9.]+)us.*/\1/p')
RPC_P99_US=$(echo "$RPC_RESULT" | sed -nE 's/.*p99=([0-9.]+)us.*/\1/p')
restore

# ============ 测试 2：10 节点动态发现（短暂验证，不上传）============
echo
echo "========== 测试 2：10 节点动态发现 =========="
check_mem
start_cluster "${DISCOVERY_PORTS[*]}" 10 "10 节点发现"
DISCOVER_COUNT="$CLUSTER_COUNT"
if [ "$DISCOVER_COUNT" != "10" ]; then
    echo "[警告] 未发现 10 个节点（count=$DISCOVER_COUNT）"
else
    echo "10 节点动态发现成功（count=$DISCOVER_COUNT）"
fi
restore

# ============ 测试 3：循环上传极限（3 节点，降载适配 2GB VM）============
echo
echo "========== 测试 3：循环上传极限（3 节点）=========="
check_mem
start_cluster "${SCALE_PORTS[*]}" 3 "3 节点规模测试"
echo "（本轮发现节点数 = ${CLUSTER_COUNT:-未检测到}）"

# 生成 1MB 基准文件
dd if=/dev/urandom of="$TMP_DIR/blob.bin" bs=1024 count=1024 2>/dev/null
echo "基准文件：1MB（1 块，CHUNK_SIZE=4MB）"

UPLOAD_MAX_N=""
for n in "${UPLOAD_SEQ[@]}"; do
    echo
    echo "----- 循环上传 $n 次 -----"
    clear_cluster_data   # 清空上一档的 MySQL 记录与落盘文件，避免 remoteName 冲突/追加污染
    ok=0; fail=0
    total_ms=0
    for i in $(seq 1 "$n"); do
        # ZooKeeper 存活守卫：内存压力下 ZK 若先崩，立即停止而非静默挂死
        if ! zk_alive; then
            echo ">>> ZooKeeper 已停止（多为内存耗尽），中止；最大成功规模 = ${UPLOAD_MAX_N:-无}，本档已完成 $ok/$n"
            restore
            exit 1
        fi
        ln -f "$TMP_DIR/blob.bin" "$TMP_DIR/up_$i.bin" 2>/dev/null || cp "$TMP_DIR/blob.bin" "$TMP_DIR/up_$i.bin"
        t0=$(date +%s.%N)
        if timeout "$TIMEOUT_UPLOAD" "$BIN/fs_caller" -i "$CONFIG/filestore_meta.cnf" upload "$TMP_DIR/up_$i.bin" 2>&1 | grep -q " done"; then
            ok=$((ok + 1))
        else
            fail=$((fail + 1))
        fi
        t1=$(date +%s.%N)
        total_ms=$(awk -v a="$total_ms" -v s="$t0" -v e="$t1" 'BEGIN { printf "%.3f", a + (e - s) * 1000 }')
        rm -f "$TMP_DIR/up_$i.bin"
    done
    avg_ms=$(awk -v t="$total_ms" -v c="$n" 'BEGIN { if (c > 0) printf "%.2f", t / c; else print "0" }')
    echo "结果：成功 $ok / $n，失败 $fail，平均 ${avg_ms} ms/次"
    if [ "$fail" -eq 0 ]; then
        UPLOAD_MAX_N="$n"
        UPLOAD_AVG_MS="$avg_ms"
        echo ">>> $n 次 100% 成功，继续加大规模"
    else
        echo ">>> $n 次出现 $fail 次失败，停止；最大成功规模 = ${UPLOAD_MAX_N:-无}"
        break
    fi
done
restore

# ============ 测试 4：大文件极限（3 节点，降载适配 2GB VM）============
echo
echo "========== 测试 4：大文件极限（3 节点）=========="
check_mem
start_cluster "${SCALE_PORTS[*]}" 3 "3 节点规模测试"
echo "（本轮发现节点数 = ${CLUSTER_COUNT:-未检测到}）"

BIGFILE_MAX_MB=""
for mb in "${BIGFILE_SEQ_MB[@]}"; do
    echo
    echo "----- 大文件 ${mb}MB -----"
    # ZooKeeper 存活守卫
    if ! zk_alive; then
        echo ">>> ZooKeeper 已停止（多为内存耗尽），中止；最大成功大小 = ${BIGFILE_MAX_MB:-无}"
        restore
        exit 1
    fi
    clear_cluster_data   # 清空上一档 big.bin 的 MySQL 记录与落盘文件，避免 remoteName 冲突/追加污染
    dd if=/dev/zero of="$TMP_DIR/big.bin" bs=1M count="$mb" 2>/dev/null
    # 上传
    t0=$(date +%s.%N)
    if ! timeout "$TIMEOUT_UPLOAD" "$BIN/fs_caller" -i "$CONFIG/filestore_meta.cnf" upload "$TMP_DIR/big.bin" > "$LOG_DIR/big_upload.log" 2>&1 \
         && grep -q " done" "$LOG_DIR/big_upload.log"; then
        echo ">>> 上传 ${mb}MB 失败，停止；最大成功大小 = ${BIGFILE_MAX_MB:-无}"
        break
    fi
    # 下载
    if ! timeout "$TIMEOUT_UPLOAD" "$BIN/fs_caller" -i "$CONFIG/filestore_meta.cnf" download big.bin > "$LOG_DIR/big_download.log" 2>&1 \
         && grep -q " done" "$LOG_DIR/big_download.log"; then
        echo ">>> 下载 ${mb}MB 失败，停止；最大成功大小 = ${BIGFILE_MAX_MB:-无}"
        break
    fi
    # md5 对比
    src_md5=$(md5sum "$TMP_DIR/big.bin" | awk '{print $1}')
    dst_md5=$(md5sum "downloads/big.bin" | awk '{print $1}')
    t1=$(date +%s.%N)
    secs=$(awk -v s="$t0" -v e="$t1" 'BEGIN { printf "%.3f", e - s }')
    thr=$(awk -v m="$mb" -v t="$secs" 'BEGIN { if (t > 0) printf "%.2f", m / t; else print "0" }')
    if [ "$src_md5" != "$dst_md5" ]; then
        echo ">>> md5 不一致（src=$src_md5 dst=$dst_md5），停止；最大成功大小 = ${BIGFILE_MAX_MB:-无}"
        break
    fi
    echo "结果：${mb}MB 上传+下载成功，md5 一致，吞吐 ${thr} MB/s（上传+下载合计计时）"
    BIGFILE_MAX_MB="$mb"
    BIGFILE_THROUGHPUT="$thr"
    BIGFILE_MD5="$src_md5"
    rm -f "$TMP_DIR/big.bin" "downloads/big.bin"
    echo ">>> ${mb}MB 成功，继续加大规模"
done
restore

# ============ 生成测试报告 ============
echo
echo "========== 生成测试报告 =========="
DATE=$(date '+%Y-%m-%d %H:%M:%S')
cat > "$REPORT" <<EOF
# 分布式文件存储系统 —— 极限测试报告

> 生成时间：${DATE}
> 测试方式：$(hostname) 本地单机，ZooKeeper 2181 + MySQL 3306；动态发现用 ${#DISCOVERY_PORTS[@]} 节点，规模测试用 ${#SCALE_PORTS[@]} 节点 + 1 个元数据服务
> 测试脚本：\`test/integration/run_new_tests.sh\`（每个测试结束后还原状态再做下一个；极限测试循序渐进、只取最大成功规模）
> 说明：规模测试降为 ${#SCALE_PORTS[@]} 节点以适配 2GB VM（10 节点 + ZK 并发会内存耗尽致 ZK 崩溃）；「最大成功规模」受测试环境内存封顶，非系统容量上限。

## 1. RPC 调用延迟（直连 FriendService，短连接）

口径：单次 RPC 往返 = TCP 建连 + 请求序列化 + 网络往返 + 响应反序列化（含握手/挥手）。

\`\`\`
${RPC_RESULT:-（未产生结果）}
\`\`\`

## 2. 存储节点动态发现（${#DISCOVERY_PORTS[@]} 节点）

- meta 后台轮询 ZooKeeper 临时节点，实测发现节点数 = **${DISCOVER_COUNT:-未检测到}**。

## 3. 循环上传极限（${#SCALE_PORTS[@]} 节点）

- 每文件 **1MB（1 块）**，次数循序 ${UPLOAD_SEQ[*]}，**最大 100% 成功规模 = ${UPLOAD_MAX_N:-无} 次**，平均 **${UPLOAD_AVG_MS:-?} ms/次**。

## 4. 大文件极限（${#SCALE_PORTS[@]} 节点）

- 大小循序 ${BIGFILE_SEQ_MB[*]} MB，**最大成功大小 = ${BIGFILE_MAX_MB:-无} MB**，下载 md5 与源文件一致（${BIGFILE_MD5:-?}），吞吐 ${BIGFILE_THROUGHPUT:-?} MB/s。

## 5. resume 回填建议（手动修改版 5 条）

1. RPC 框架层：单次 RPC 往返延迟 ~${RPC_AVG_US:-?}µs（本地回环短连接实测，中位 ${RPC_MEDIAN_US:-?}µs / P99 ${RPC_P99_US:-?}µs）。
2. 元数据服务：循环上传 1MB 文件 ${UPLOAD_MAX_N:-N} 次成功率 100%（实测）。
3. 存储服务：逐块 MD5 校验（4MB 块）；单文件 ${BIGFILE_MAX_MB:-N}MB 大文件分块流式读写（实测）。
4. 数据分布优化：约 1/N（理论值，不改）。
5. 传输效率优化：连接建立次数从块数降至节点数（会话复用，1GB 文件 256 块→3 连接）；吞吐 ${BIGFILE_THROUGHPUT:-?} MB/s（端到端实测）。
EOF
echo "报告已写入 $REPORT"
echo
echo "========== 全部测试完成 =========="
