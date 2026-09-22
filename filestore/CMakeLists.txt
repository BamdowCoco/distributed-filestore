# 分布式文件存储系统（基于 mpRPC 框架的应用场景）

# 让 filestore/ 目录加入头文件搜索路径，便于 include "file_storage.pb.h"
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# 元数据服务端（文件索引，MySQL 持久化）
add_executable(meta_callee
    callee/meta_service.cc
    callee/database/Connection.cc
    callee/database/CommonConnectionPool.cc
    file_storage.pb.cc
)
# meta_callee 通过一致性哈希用到 md5Hex（OpenSSL MD5）。mysqlclient 编译时依赖
# /usr/local/lib 的 libssl（启用 IDEA），而系统 /usr/lib 的 libcrypto 禁用了 IDEA，
# 二者符号不匹配（EVP_idea_cbc）。故显式链接 /usr/local/lib 的完整版 libcrypto。
target_link_libraries(meta_callee mprpc protobuf::libprotobuf mysqlclient /usr/local/lib/libcrypto.so.1.1)

# 存储服务端（文件块落盘）
add_executable(storage_callee
    callee/storage_service.cc
    file_storage.pb.cc
)
target_link_libraries(storage_callee mprpc protobuf::libprotobuf crypto)

# 客户端（上传/下载/删除）
add_executable(fs_caller
    caller/fs_caller.cc
    file_storage.pb.cc
)
target_link_libraries(fs_caller mprpc protobuf::libprotobuf crypto)
