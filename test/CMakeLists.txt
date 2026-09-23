# 测试工具（本地基准中仅 bench_rpc_latency 需要链接框架库，其余由 run_tests.sh 用 g++ 单独编译）

# RPC 延迟基准：直连 FriendService（复用 example 的 friend.pb / user.pb），循环测往返延迟
add_executable(bench_rpc_latency
    bench/bench_rpc_latency.cc
    ${PROJECT_SOURCE_DIR}/example/friend.pb.cc
    ${PROJECT_SOURCE_DIR}/example/user.pb.cc
)
target_link_libraries(bench_rpc_latency mprpc protobuf::libprotobuf)
