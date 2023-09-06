//
// Created by 张晋铭 on 2023/8/12.
//
#include "tpcc.h"
#include "clock.h"
#include "config.h"
#include "table.h"

const std::string FILE_NAME = "../rmdb_client/build/tpcc_test.sql";

void generate_new_orders() {
    std::ofstream outfile;
    outfile.open(FILE_NAME, std::ios::out | std::ios::app);
    outfile.close();
}

void generate_payment() {
    std::ofstream outfile;
    outfile.open(FILE_NAME, std::ios::out | std::ios::app);
    outfile.close();
}

void generate_delivery() {
    std::ofstream outfile;
    outfile.open(FILE_NAME, std::ios::out | std::ios::app);
    outfile.close();
}

void generate_order_status() {
    std::ofstream outfile;
    outfile.open(FILE_NAME, std::ios::out | std::ios::app);
    outfile.close();
}

void generate_stock_level() {
    std::ofstream outfile;
    outfile.open(FILE_NAME, std::ios::out | std::ios::app);
    outfile << "select d_next_o_id from district where d_id=:d_id and d_w_id=:w_id;\n";
    outfile.close();
}