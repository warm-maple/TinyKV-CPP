cmake_minimum_required(VERSION 3.10)
project(TinyKV)

set(CMAKE_CXX_STANDARD 17)

# 关键修改：路径变了，要指向 src/include
include_directories(src/include)

# 引入 GoogleTest
find_package(GTest REQUIRED)

# 编译测试文件
add_executable(main test/t/main.cpp)

# 链接 GTest 和 线程库
target_link_libraries(main GTest::GTest GTest::Main pthread)