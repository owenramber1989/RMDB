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

#include "execution_defs.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
   public:
    Rid _abstract_rid;

    Context *context_;

    std::unique_ptr<AbstractExecutor> prev_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    [[nodiscard]] virtual const std::vector<ColMeta> &cols() const = 0;
    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool has_aggre() { return false; };
    virtual std::string get_nickname() { return ""; };
    virtual void set_conds(std::vector<Condition> conds) {} ;
    virtual bool is_end() const { return true; };
    virtual void feed(const std::map<TabCol, Value> &feed_dict) = 0;
    virtual ast::AggregationType get_aggre_type() {};
    // virtual void feed(const std::map<TabCol, Value> &feed_dict) = 0;

    virtual std::vector<std::unique_ptr<RmRecord>> get_block() {};
    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};
    virtual bool has_nlj() {};
    virtual std::string get_tbl_name() {};
    virtual size_t get_sort_offset() {};
    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            if(target.tab_name==""){
                return col.name==target.col_name;
            } else {
                return col.tab_name == target.tab_name && col.name == target.col_name;
            }
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    virtual void fast_feed(const std::map<TabCol, std::vector<Value>> &feed_dict, size_t cnt) {};
    virtual TabMeta get_tables() {}
    // 对于每一个表列，我们都将他们的值加入到map里面，并返回这个map
    std::map<TabCol, Value> rec2dict(const std::vector<ColMeta> &cols, const RmRecord *rec) {
        std::map<TabCol, Value> rec_dict;
        for (auto &col : cols) {
            TabCol key{col.tab_name, col.name};
            Value val;
            // 目前的问题是，左表是空表的时候也会进行下一步
            auto val_buf = rec->data + col.offset;
            if (col.type == TYPE_INT) {
                // std::cout<<(*(int*)val_buf)<<std::endl;
                // if((*(std::string *)val_buf).empty()) continue;
                val.set_real_int(*(int*)val_buf);
            } else if (col.type == TYPE_FLOAT) {
                val.set_float(*(float *)val_buf);
            } else if (col.type == TYPE_STRING) {
                std::string str_val((char *)val_buf, col.len);
                str_val.resize(strlen(str_val.c_str()));
                val.set_str(str_val);
            } else if (col.type == TYPE_BIGINT) {
                // if((*(std::string *)val_buf).empty()) continue;
                val.set_real_bigint(*(std::int64_t *)val_buf);
            } else if(col.type == TYPE_DATETIME) {
                std::string str_val((char *)val_buf, col.len);
                str_val.resize(strlen(str_val.c_str()));
                val.set_str(str_val);
            }
            assert(rec_dict.count(key) == 0);
            val.init_raw(col.len);
            rec_dict[key] = val;
        }
        return rec_dict;
    }
};