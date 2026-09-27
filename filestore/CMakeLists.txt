# 分布式文件存储系统（基于 mpRPC 框架的应用场景）

# 让 filestore/ 与 filestore/callee/ 加入头文件搜索路径，
# 便于 include "file_storage.pb.h" 及 callee 下的 redis/、database/ 等
include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/callee)

# 元数据服务端（文件索引，MySQL 持久化）
add_executable(meta_callee
    callee/meta/meta_service.cc
    callee/meta/main.cc
    callee/database/Connection.cc
    callee/database/CommonConnectionPool.cc
    callee/database/dao/schema.cc
    callee/database/dao/user_dao.cc
    callee/database/dao/file_meta_dao.cc
    callee/database/dao/file_node_dao.cc
    callee/database/dao/file_chunk_dao.cc
    callee/database/dao/cleanup_queue_dao.cc
    callee/redis/redis.cpp
    file_storage.pb.cc
)
# meta_callee 通过一致性哈希用到 md5Hex（OpenSSL MD5）。mysqlclient 编译时依赖
# /usr/local/lib 的 libssl（启用 IDEA），而系统 /usr/lib 的 libcrypto 禁用了 IDEA，
# 二者符号不匹配（EVP_idea_cbc）。故显式链接 /usr/local/lib 的完整版 libcrypto。
# 待清理队列用到 Redis Stream（hiredis）。
target_link_libraries(meta_callee mprpc protobuf::libprotobuf mysqlclient /usr/local/lib/libcrypto.so.1.1 /usr/local/lib/libhiredis.so)

# 存储服务端（文件块落盘）
add_executable(storage_callee
    callee/storage/storage_service.cc
    callee/storage/main.cc
    file_storage.pb.cc
)
target_link_libraries(storage_callee mprpc protobuf::libprotobuf crypto)

# 客户端（上传/下载/删除）
add_executable(fs_caller
    caller/main.cc
    caller/fs_client.cc
    file_storage.pb.cc
)
target_link_libraries(fs_caller mprpc protobuf::libprotobuf crypto)

# 票据密钥生成工具：产出 Ed25519 密钥对并打印 kid（kid 的算法与元数据/存储节点
# 共用 ticket::TicketKey 这一份实现，避免两边漂移）
add_executable(ticket_keygen
    tools/ticket_keygen.cc
)
target_link_libraries(ticket_keygen crypto)
