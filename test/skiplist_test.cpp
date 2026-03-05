#include "../Include/SkipList.h"
#include <bits/stdc++.h>
#include <string>
#include <iostream>

int main() {
    std::cout << ">>> 数据库启动中，正在尝试从 WAL 恢复数据..." << std::endl;
    // 实例化引擎。注意：由于我们在内部用了 std::stoi 和 std::to_string，
    // 这里我们强制指定 K 为 int，V 为 std::string
    SkipList<int, std::string> kvStore;

    // ==========================================
    // 第一次运行：请取消下面三行的注释，将数据写入硬盘
    // ==========================================
    std::cout << "\n>>> 模拟接收客户端写请求..." << std::endl;
    kvStore.insert(1, "ByteDance");
    kvStore.insert(2, "Tencent");
    kvStore.remove(2); 

    // ==========================================
    // 第二次运行（断电重启模拟）：
    // 将上面三行重新注释掉，运行下面代码，见证奇迹
    // ==========================================
    std::cout << "\n>>> 模拟客户端查询请求..." << std::endl;
    std::string val;
    if (kvStore.search(1, val)) {
        std::cout << "-> 成功查找到 Key 1: " << val << std::endl;
    } else {
        std::cout << "-> 未找到 Key 1" << std::endl;
    }

    if (!kvStore.search(2, val)) {
        std::cout << "-> Key 2 确实已被删除（墓碑生效）" << std::endl;
    }

    // 打印跳表结构
    kvStore.display();

    return 0;
}