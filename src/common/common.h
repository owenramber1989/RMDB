/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <sstream>
#include <vector>
#include "defs.h"
#include "record/rm_defs.h"


struct TabCol {
    std::string tab_name;
    std::string col_name;

    friend bool operator<(const TabCol &x, const TabCol &y) {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }
    friend std::ostream& operator<<(std::ostream& os, const TabCol& tc) {
        os << "TabCol { ";
        os << "tab_name: " << tc.tab_name << ", ";
        os << "col_name: " << tc.col_name;
        os << " }";
        return os;
    }
};




struct Value {
    ColType type;  // type of value
    union {
        int int_val;      // int value
        float float_val; // float value
        std::int64_t bigint_val;
    };
    std::string str_val;  // string value

    static bool isValidDateTime(std::string str_){
        if(str_.size() != 19) return false; // If not of form 'YYYY-MM-DD HH:MM:SS'

        std::vector<int> days_in_month = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

        int year, month, day, hour, minute, second;
        std::replace(str_.begin(), str_.end(), '-', ' ');
        std::replace(str_.begin(), str_.end(), ':', ' ');

        std::stringstream ss(str_);
        ss >> year >> month >> day >> hour >> minute >> second;

        if(year < 1000 || year > 9999) return false;
        if(month < 1 || month > 12) return false;
        if(day < 1) return false;
        if(hour < 0 || hour > 23) return false;
        if(minute < 0 || minute > 59) return false;
        if(second < 0 || second > 59) return false;

        if((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) days_in_month[2] = 29;
        if(day > days_in_month[month]) return false;

        return true;
    }
    friend std::ostream& operator<<(std::ostream& os, const Value& val) {
        switch(val.type) {
            case TYPE_INT:
                os << "Value { type: int, value: " << val.int_val << " }";
                break;
            case TYPE_FLOAT:
                os << "Value { type: float, value: " << val.float_val << " }";
                break;
            case TYPE_BIGINT:
                os << "Value { type: bigint, value: " << val.bigint_val << " }";
                break;
            case TYPE_STRING:
                os << "Value { type: string, value: " << val.str_val << " }";
                break;
            case TYPE_DATETIME:
                os << "Value { type: datetime, value: " << val.str_val << " }";
                break;
            default:
                os << "Value { type: unknown }";
                break;
        }
        return os;
    }
    std::shared_ptr<RmRecord> raw;  // raw record buffer

    void set_int(std::string str_val_) {
        try {

            int int_val_ = std::stoi(str_val_);
            type = TYPE_INT;
            int_val = int_val_;
        } catch (std::out_of_range&) {
            try {
                std::int64_t bigint_val_ = std::stoll(str_val_);
                type = TYPE_BIGINT;
                bigint_val = bigint_val_;
            } catch (std::out_of_range&) {
                throw InternalError("number exceeds limit");
            }
        }
    }

    void set_real_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_bigint(int int_val_){
        type = TYPE_BIGINT;
        auto tmp = static_cast<std::int64_t>(int_val_);
        bigint_val = tmp;
    }

    void set_real_bigint(std::int64_t big_val_){
        type = TYPE_BIGINT;
        bigint_val = big_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    /*
    void set_bigint(std::int64_t bigint_val_) {
        type = TYPE_BIGINT;
        bigint_val = bigint_val_;
    }
    */
    void set_datetime(std::string str_val_) {
        if(!isValidDateTime(str_val_)) {
            throw InvalidDatetime();
        }
        type = TYPE_DATETIME;
        str_val = std::move(str_val_);
    }

    void init_raw(int len) {
        // assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT) {
            // assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;
        } else if (type == TYPE_FLOAT) {
            // assert(len == sizeof(float));
            *(float *)(raw->data) = float_val;
        } else if (type == TYPE_STRING) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        } else if (type == TYPE_DATETIME) {
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        } else if (type == TYPE_BIGINT) {
            // assert(len == sizeof(std::int64_t));
            *(std::int64_t *)(raw->data) = bigint_val;
        }
    }
};

enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };

struct Condition {
    TabCol lhs_col;   // left-hand side column
    CompOp op;        // comparison operator
    bool is_rhs_val;  // true if right-hand side is a value (not a column)
    TabCol rhs_col;   // right-hand side column
    Value rhs_val;    // right-hand side value
    std::string to_string() {
        std::stringstream ss;
        ss << "Condition { ";
        ss << "lhs_col: " << this->lhs_col << ", ";
        ss << "op: " << this->op << ", ";
        ss << "is_rhs_val: " << (this->is_rhs_val ? "true" : "false") << ", ";
        if (this->is_rhs_val) {
            ss << "rhs_val: " << this->rhs_val;
        } else {
            ss << "rhs_col: " << this->rhs_col;
        }
        ss << " }";
        return ss.str();
    }
};

struct SetClause {
    TabCol lhs;
    Value rhs;
    bool exp = false;
    bool add = false;
};