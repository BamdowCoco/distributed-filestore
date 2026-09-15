cmake_minimum_required(VERSION 3.12)
project(mprpc)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_BUILD_TYPE "Debug")

# 查找 Protobuf 依赖
find_package(Protobuf CONFIG REQUIRED)
find_package(absl REQUIRED)

# 设置项目可执行文件输出目录
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
# 设置项目库文件输出目录
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

# 头文件搜索路径
include_directories(
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/example
)

# 库文件搜索路径
link_directories(
    ${PROJECT_SOURCE_DIR}/lib
)

# src 包含mprpc框架相关代码
add_subdirectory(${PROJECT_SOURCE_DIR}/src)
# example 包含了mprpc框架使用的示例代码
add_subdirectory(${PROJECT_SOURCE_DIR}/example)
# filestore 分布式文件存储系统（框架应用场景）
add_subdirectory(${PROJECT_SOURCE_DIR}/filestore)