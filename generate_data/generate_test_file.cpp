//
// Created by 张晋铭 on 2023/8/12.
//
#include "tpcc.h"

#include <iostream>
#include <random>

int generate_txn() {
    std::random_device rd; // 获取随机数种子
    std::mt19937 gen(rd()); // 使用Mersenne Twister算法生成随机数
    std::discrete_distribution<> distribution({10.0/23, 10.0/23, 1.0/23, 1.0/23, 1.0/23}); // 设置概率分布

    int number = distribution(gen);
    switch (number) {
        case 0:
            generate_new_orders();
        case 1:
            generate_payment();
        case 2:
            generate_delivery();
        case 3:
            generate_order_status();
        case 4:
            generate_stock_level();
        default:
            return -1; // 永远不应该发生，仅用于错误检查
    }
}



int main() {
    for (int i = 0; i < 23; ++i) { // 生成100个随机数作为示例
        generate_txn();
    }
    return 0;
}
