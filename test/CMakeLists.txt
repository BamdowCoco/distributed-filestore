# 测试工具（本地基准中仅 bench_rpc_latency 需要链接框架库，其余由 run_tests.sh 用 g++ 单独编译）

# RPC 延迟基准：直连 FriendService（复用 example 的 friend.pb / user.pb），循环测往返延迟
add_executable(bench_rpc_latency
    bench/bench_rpc_latency.cc
    ${PROJECT_SOURCE_DIR}/example/friend.pb.cc
    ${PROJECT_SOURCE_DIR}/example/user.pb.cc
)
target_link_libraries(bench_rpc_latency mprpc protobuf::libprotobuf)

# filestore 第四轮（正确性与安全）修复的针对性验证：直接构造 RPC 请求，断言服务端
# 的拒绝/接受行为（越权、无鉴权、票据不匹配、越界输入等 CLI 覆盖不到的路径）。
# 运行前置：ZooKeeper + MySQL + Redis，且已启动 meta_callee 与至少一个 storage_callee。
add_executable(fs_security_test
    integration/verify_round4.cc
    ${PROJECT_SOURCE_DIR}/filestore/file_storage.pb.cc
)
target_include_directories(fs_security_test PRIVATE ${PROJECT_SOURCE_DIR}/filestore)
target_link_libraries(fs_security_test mprpc protobuf::libprotobuf crypto)
