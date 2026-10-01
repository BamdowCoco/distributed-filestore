# 简历量化指标测试说明

本目录即测试代码所在目录，用于实测 [project-resume.md](../docs/filestore/project-resume.md) 中「手动修改版」5 条指标的量化数字，把简历里「约」字估算的数字变成可复现的实测数字。

## 指标 ↔ 测试入口对照

| 简历指标 | 简历数字 | 测试入口 | 类型 |
|---|---|---|---|
| RPC 往返延迟 | （手动修改版第 1 条） | `test/bench/bench_rpc_latency.cc` + `run_new_tests.sh` 测试 1 | 本地基准 + 集成 |
| 循环上传规模 / 成功率 | （手动修改版第 2 条） | `test/integration/run_new_tests.sh` 测试 2 | 集成（10 节点） |
| 大文件读写规模 | （手动修改版第 3 条） | `test/integration/run_new_tests.sh` 测试 3 | 集成（10 节点） |
| 单块 MD5 校验耗时 | （简历不提） | `test/bench/bench_md5.cc` | 本地基准 |
| 块重映射 | 约 1/N | `test/bench/test_consistent_hash.cc` | 本地基准 |
| 连接开销降低 | 约 90% | `test/bench/bench_connect.cc` + `test/integration/run_integration.sh` | 本地基准 + 集成 |
| 空间利用率提升 | 约 50% | `test/bench/test_compact_storage.cc` | 本地基准 |
| 吞吐量提升 | 约 40% | `test/bench/bench_throughput.cc` + `test/integration/run_integration.sh` | 本地基准 + 集成 |

> 最新一轮端到端极限实测（RPC 延迟、10 节点发现、循环上传最大规模、大文件最大大小）的汇总数字见 [test-report.md](test-report.md)，由 `run_new_tests.sh` 每次运行后自动生成。

## 一、本地基准（不依赖服务，直接跑）

### 运行

```bash
bash test/bench/run_tests.sh
```

脚本用 `g++ -O2 -std=c++11` 编译 5 个程序到 `test/bench/build/` 并依次运行。仅前 3 个用到 `filestore/common`（`md5Hex`/`ConsistentHash`）需 `-lcrypto`，后 2 个纯标准库。

### 各程序输出与解读（2026-09-22 实测）

**1. bench_md5 —— 单块校验耗时**

```
平均每次 md5Hex = 1.2182 us   （20 万次循环，1024 字节块）
```

- **结论**：纯 `md5Hex`（OpenSSL MD5）计算约 **1.2 µs**。
- **与简历「约 5ms」的关系**：两者口径不同。`md5Hex` 只是校验计算本身；简历的 5ms 是「一次块上传」的端到端耗时（含 TCP 往返、序列化、落盘）。本测试给出的是校验计算的开销下限，说明 MD5 校验不是瓶颈。若想测端到端单块耗时，可用 `strace -T` 或 `perf stat` 对单次 `PutChunk` RPC 计时。

> 注：框架分块 `CHUNK_SIZE` 现为 4MB；本基准与 `test_compact_storage` 已解耦为局部 1KB 单元块（`BLOCK_SIZE=1024`），测的是 per-byte 成本（约 1.2ns/字节），4MB 块 MD5 约 4.8ms。

**2. test_consistent_hash —— 块重映射**

```
节点数 3 -> 4（新增 1 节点），10000 个块 key：
[一致性哈希] 重映射占比 = 22.37%
[取模]       重映射占比 = 74.98%
```

- **结论**：一致性哈希重映射约 **1/4 ≈ 22%**，取模接近全量（**75%**）。与简历「约 1/N」一致（N+1=4 节点时理论 1/4）。

**3. bench_connect —— 连接开销**

```
消息数 M = 500（模拟 500 块）：
[优化前 每块新建连接] connect 次数 = 500，总耗时 = 30.57 ms
[优化后 复用一条连接] connect 次数 = 1，  总耗时 = 18.92 ms
connect 次数降低 = 99.8%（500 -> 1）
```

- **结论**：connect 次数从块数降到 1，降低 **(M-1)/M**；对应真实场景「连接次数从块数 N 降到节点数 M」。(N-M)/N 随 N/M 增大趋近 100%，简历「约 90%」是保守值。

**4. test_compact_storage —— 空间利用率**

```
节点1 负责 475 块（一致性哈希分配，块号稀疏）：
固定偏移写落盘 = 1024000 字节
紧凑追加写落盘 = 486400 字节
空间利用率提升 = 52.5%
```

- **结论**：固定偏移写（`offset = chunk_index * CHUNK_SIZE`）在稀疏块号下打出大量空洞；紧凑追加写落盘大小 = 实际写入字节之和。2 节点场景空洞占 **52.5%**，简历「约 25%」是保守值（空洞占比随节点数/块分布变化）。

**5. bench_throughput —— 吞吐量**

```
块数 N = 4096（总 4 MB），复用同一条连接：
[优化前 逐块往返] 吞吐 = 28.80 MB/s
[优化后 批量往返] 吞吐 = 90.53 MB/s
```

- **结论**：批量聚合把 RPC 往返从 N 次降到 1 次，本地回环下吞吐提升约 **3.1 倍**。这隔离了磁盘 I/O 与 MySQL 后的纯往返收益上限。

## 二、集成测试（真实端到端，需 ZooKeeper + MySQL）

### 前置条件

1. ZooKeeper 已在 `2181` 监听、MySQL 已在 `3306` 监听；
2. `./autobuild.sh` 已构建出 `bin/{meta_callee,storage_callee,fs_caller}`；
3. `config/mysql.cnf` 已配置真实数据库（参考 `config/mysql.cnf.example`，**该文件不提交**）。

### 运行

```bash
bash test/integration/run_integration.sh
```

脚本会：启动 2 个存储节点（8001/8002）+ 元数据服务（8003）→ 生成 1MB 随机文件 → 用 `strace` 统计 connect 次数、`date +%s.%N` 计时吞吐 → 清理进程与落盘。

### 输出与解读

> 说明：本脚本现用 4MB 块（16MB 文件 = 4 块）。连接开销与吞吐量的最新端到端实测以 [test-report.md](test-report.md)（`run_new_tests.sh`，1GB 大文件 3 节点）为准。

```
测试文件：16777216 字节（4 块，CHUNK_SIZE=4MB）

[连接开销] 优化后：连到存储节点的 connect 次数 = 2（批量聚合/会话复用，每节点 1 次）
           优化前（理论）：每块新建连接 = 4 次
           连接开销降低 = 50%（4 -> 2，块数 -> 节点数）

[吞吐量]   见 run_new_tests.sh 大文件档
```

- **连接开销**：真实上传 4 块、2 节点，优化后 2 次 connect（每节点 1 次）。大文件下（1GB = 256 块）连接数从块数降到节点数，降低 98.8%（见 `run_new_tests.sh`）。
- **吞吐量**：端到端吞吐由 `run_new_tests.sh` 大文件档实测（1GB 文件 3 节点 45.73 MB/s），含 MySQL 元数据事务、ZooKeeper 发现、落盘。

### 极限 / 规模实测（run_new_tests.sh）

统一脚本 [test/integration/run_new_tests.sh](integration/run_new_tests.sh) 一次性执行 4 个「未做过」的测试，每个测试结束后**还原状态**（杀进程 → 等 ZK 临时节点过期约 35s → 清 MySQL → 删落盘/下载/临时文件）再做下一个，最后生成 [test-report.md](test-report.md)。

```bash
bash test/integration/run_new_tests.sh
```

四个测试依次为：

1. **RPC 调用延迟**：起 `bin/callee`（FriendService，8000），`bench_rpc_latency` 直连循环 `GetFriendList` 10000 次，输出平均 / 中位 / P99 延迟（口径：短连接建连 + 序列化 + 往返 + 反序列化）。
2. **10 节点动态发现**：起 10 个 `storage_callee` + 1 个 `meta_callee`，grep `meta.log` 的 `refreshed storage nodes, count:10` 验证动态发现后立即还原，**不做上传**。
3. **循环上传极限（3 节点）**：起 3 个 `storage_callee` + 1 个 `meta_callee`，循环上传 **1MB 文件**，次数按 `UPLOAD_SEQ=(100 500 1000 2000 5000 10000)` 循序渐进，**某档出现失败即停，取上一个 100% 成功的次数**为最大成功规模。
4. **大文件极限（3 节点）**：大文件按 `BIGFILE_SEQ_MB=(64 256 512 1024)` 循序渐进（到 1GB），每档上传 → 下载 → `md5sum` 比对，**取最大成功且 md5 一致的大小**，并计时吞吐。

**大文件为什么能到 1GB**（[filestore/common/common.h](../filestore/common/common.h) 分块 4MB + [filestore/caller/fs_caller.cc](../filestore/caller/fs_caller.cc) 拆批 + 会话复用）：框架层有 64MB 请求/响应上限，若「每节点一个巨型批量请求」则大文件封顶在 ~192MB。故分块取 4MB（1GB=256 块），并把每节点的块按 `MAX_BATCH_CHUNKS=15`（60MB 载荷）拆成多个子批顺序发送，单批内存有界；同时 [include/mprpc_channel.h](../include/mprpc_channel.h) 直连模式支持会话内复用一条连接（`MprpcChannel(ip, port, /*sessionReuse=*/true)`，客户端主动 close），连接建立次数从块数降到节点数。

**为什么规模测试降到 3 节点**：本测试运行在 **2GB 内存 VM** 上，10 个 `storage_callee`（每个 4 个 muduo I/O 线程）+ meta + ZooKeeper（JVM）+ MySQL + 桌面/编辑器进程并发会内存耗尽，导致 ZooKeeper 先被 GC/swap 冻结、连接超时（`zk retcode=-7`）而崩溃——这是测试环境封顶，非存储系统容量上限。故把「10 节点」与「规模压测」解耦：10 节点只做短暂的发现验证，持续压测（循环上传 / 大文件）用 3 节点。

**脚本内置两个守卫**：
- **内存守卫**：起集群前读 `/proc/meminfo` 的 `MemAvailable`，<512MB 告警、<256MB 直接中止。
- **ZK 存活守卫**：循环上传/大文件每档每步前用 `ss` 检查 ZooKeeper 是否仍在 2181 监听，ZK 一挂立即记录已成功数、还原并退出，不再静默挂死。

**ZooKeeper 小堆启动**（2GB VM 强烈建议，限制 JVM 堆避免 GC 抖动）：

```bash
SERVER_JVMFLAGS="-Xms128m -Xmx256m" zkServer.sh start
```

**「循序渐进、只取成功」的读数方法**：报告第 3/4 节的「最大成功规模 / 最大成功大小」即是最接近真实极限的实测值，而非一次性跑到固定数字——因为单个 RPC 请求有 64MB 上限、批量聚合内存随文件增大而增长，实际以最大成功档为准（本环境实测 1GB 通过）。

**可调参数**（冒烟 / 快速验证时用环境变量覆盖成小序列）：

```bash
UPLOAD_SEQ_OVERRIDE="3 5" BIGFILE_SEQ_MB_OVERRIDE="4 8" bash test/integration/run_new_tests.sh
```

## 三、实测 vs 简历数字对照与回填建议

| 手动修改版条目 | 简历数字 | 实测 |
|---|---|---|
| 1. RPC 框架层 | 单次往返延迟 | ~178µs（本地回环短连接，中位 ~149µs / P99 ~520µs） |
| 2. 元数据服务 | 循环上传成功率 | 1MB 文件 2000 次 100%，平均 ~31ms/次 |
| 3. 存储服务 | 大文件读写规模 | 1GB 上传+下载 md5 一致，吞吐 ~46 MB/s |
| 4. 数据分布优化 | 约 1/N | 一致性哈希 3→4 节点重映射 22.4%（≈1/4，理论 1/4） |
| 5. 传输效率优化 | 连接数降低 / 吞吐 | 1GB 文件 256 块→3 连接（降低 98.8%）；吞吐 45.73 MB/s |

> 附本地基准（不依赖服务，1KB 单元块）：单块 MD5 1.2µs；空间利用率提升 52.5%；批量 vs 逐块往返吞吐 3.1 倍。这些数字不随框架分块 4MB 变化，仍可引用。

> 说明：上表「实测」均由 `run_new_tests.sh`（第 1/2/3/4 节）与本地基准可复现，面试按「口径（节点数、块数、文件大小）」自圆其说即可。

## 四、测试过程中发现并修复的真实缺陷

集成测试最初无法跑通，暴露了两个真实缺陷，已修复：

1. **TCP 半包导致大请求解析失败**（[src/mprpc_provider.cc](../src/mprpc_provider.cc)）
   - 现象：`PutChunksBatch` 批量上传 493KB 请求被 muduo 拆成多次 `onMessage` 回调，旧实现用 `retrieveAllAsString()` 一次性取走并假设已收全，导致 args 截断、`ParseFromString` 失败，触发客户端重试后回滚。
   - 修复：改为「peek 判断 `header_size + header + args` 是否收全 → 收全才 retrieve」的循环分帧，并增加 headerSize/argsSize 上限校验。小请求（单块 PutChunk）此前未暴露该问题，批量上传才触发。

2. **旧库缺新列导致提交失败**（[filestore/callee/meta_service.cc](../filestore/callee/meta_service.cc)）
   - 现象：`CREATE TABLE IF NOT EXISTS` 不会给已存在的旧 `file_chunk` 表补列，`CommitUpload` 写 `offset/size` 时报 `Unknown column 'offset'`。
   - 修复：启动时用 `information_schema.COLUMNS` 检测并 `ALTER TABLE ... ADD COLUMN` 幂等补齐 `checksum/offset/size` 三列。

3. **批量大请求内存泄漏导致大文件 OOM**（[src/mprpc_provider.cc](../src/mprpc_provider.cc)）
   - 现象：`RpcProvider::onMessage` 里 `request`/`response` 用 `New()` 分配后从未 `delete`，每个批量请求都泄漏整个请求体（含全部块数据）。之前 64MB 上限把单请求压得很小未暴露；改为 4MB 块 + ≤64MB 子批后，1GB 文件上传让存储节点 RSS 涨到 1.14GB 被 OOM killer 杀掉（`dmesg` 可见 `Out of memory: Killed process storage_callee`）。
   - 修复：`request` 在 `service->CallMethod` 返回后 `delete`；`response` 在 `sendRpcResponse` 序列化发送后 `delete`（错误路径也释放）。修复后 1GB 大文件上传+下载 md5 一致通过。

## 五、通用质量工具（附录，可选深入）

| 目标 | 工具 | 方法 |
|---|---|---|
| 内存峰值（流式 vs 全量） | `valgrind --tool=massif` | 上传同一大文件对比峰值曲线 |
| 内存泄漏（连接池/上传路径） | `valgrind --tool=memcheck --leak-check=full` | 跑一轮上传/下载/删除后退出 |
| CPU 热点 | `perf record` / `gprof`（`-pg`） | 对上传流程采样 |
| SQL 效率 | MySQL `EXPLAIN` | 分析 SELECT 是否命中 `(filename, chunk_index)` 主键 |
