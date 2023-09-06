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
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段
    std::map<TabCol, Value> _prev_feed_dict;
    std::vector<Condition> fed_conds_;          // join条件
    bool endend = false;
    std::unique_ptr<RmRecord> cur = std::make_unique<RmRecord>();
    std::vector<std::unique_ptr<RmRecord>> lhs_block_;  // 添加一个变量来存储一块数据
    std::vector<std::unique_ptr<RmRecord>> rhs_block_;  // 添加一个变量来存储一块数据
    int l_size_;
    int r_size_;
    int l_cnt = 0; // 标记当前在buffer的下标
    int r_cnt = 0;
    std::string tab_name_;
    std::vector<ColMeta> r_cols;
    std::map<TabCol, std::vector<Value>> all_dict_;
    int l_len;
    int r_len;

   public:
    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_; }
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        fed_conds_ = std::move(conds);
        r_cols = right_->cols();
        tab_name_ = right_->get_tbl_name();
        l_len = left_->tupleLen();
        r_len = right_->tupleLen();
    }
    void setDict() {
        auto cols = left_->cols();
        // 遍历lhs_block_中的每一个RmRecord
        for (auto& rec : lhs_block_) {
            // 遍历每一个列
            for (auto& col : cols) {
                TabCol key{col.tab_name, col.name};
                Value val;

                auto val_buf = rec->data + col.offset;
                if (col.type == TYPE_INT) {
                    val.set_real_int(*(int*)val_buf);
                } else if (col.type == TYPE_FLOAT) {
                    val.set_float(*(float *)val_buf);
                } else if (col.type == TYPE_STRING) {
                    std::string str_val((char *)val_buf, col.len);
                    str_val.resize(strlen(str_val.c_str()));
                    val.set_str(str_val);
                } else if (col.type == TYPE_BIGINT) {
                    val.set_real_bigint(*(std::int64_t *)val_buf);
                } else if(col.type == TYPE_DATETIME) {
                    std::string str_val((char *)val_buf, col.len);
                    str_val.resize(strlen(str_val.c_str()));
                    val.set_str(str_val);
                }
                val.init_raw(col.len);
                // 将val添加到map对应列的对应vector的末尾
                all_dict_[key].push_back(val);
            }
        }
    }
    ColMeta get_col_offset(const TabCol &target) override {
        try {
            // 尝试在 left_ 对象上调用函数
            return left_->get_col_offset(target);
        }
        catch (...) {
            // 如果 left_ 对象上的调用引发异常，再在 right_ 对象上调用函数
            return right_->get_col_offset(target);
        }
    }
    void feed(const std::map<TabCol, Value> &feed_dict) override {
        _prev_feed_dict = feed_dict; // 记录一下前一个扫描算子使用了哪些表列值的map
        left_->feed(feed_dict);
    }
    std::vector<std::string> get_tbl_names(){
        std::vector<std::string> tbls;
        for(const auto& col:cols_){
            tbls.push_back(col.tab_name);
        }
        return tbls;
    }
    void set_conds(std::vector<Condition> conds) override {
        fed_conds_ = conds;
    }

    bool eval_cond(const std::vector<ColMeta> &rec_cols,
                   const Condition &cond,
                   const RmRecord *rec) {
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        auto *lhs = rec->data + lhs_col->offset;
        char* rhs;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs = cond.rhs_val.raw->data;
        } else {
            // rhs is a column
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs = rec->data + rhs_col->offset;
        }
        if(rhs_type != lhs_col->type){
            if(rhs_type==TYPE_DATETIME&&lhs_col->type==TYPE_STRING&&lhs_col->len>=19) {
            } else {
                throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(rhs_type));
            }
        }  // TODO convert to common type
        int cmp = ix_compare(lhs, rhs, rhs_type, lhs_col->len);
        if (cond.op == OP_EQ) {
            return cmp == 0;
        } else if (cond.op == OP_NE) {
            return cmp != 0;
        } else if (cond.op == OP_LT) {
            return cmp < 0;
        } else if (cond.op == OP_GT) {
            return cmp > 0;
        } else if (cond.op == OP_LE) {
            return cmp <= 0;
        } else if (cond.op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Unexpected op type");
        }
    }

    bool eval_conds(const std::vector<ColMeta> &rec_cols,
                    const std::vector<Condition> &conds,
                    const RmRecord *rec) {
        return std::all_of(conds.begin(), conds.end(), [&](const Condition &cond) {
            return eval_cond(rec_cols, cond, rec);
        });
    }

    void beginTuple() override {
        lhs_block_ = left_->get_block();
        if(lhs_block_.empty()) {
            endend = true;
            return;
        }
        rhs_block_ = right_->get_block();
        l_size_ = lhs_block_.size();
        r_size_ = rhs_block_.size();
        setDict();
        static std::map<CompOp, CompOp> swap_op_ack = {
                {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };
        for (auto &cond : fed_conds_) {
            if(cond.lhs_col.tab_name != tab_name_) {
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op_ack.at(cond.op);
            }
        }

        for(;l_cnt<l_size_;l_cnt++){
            for (auto &cond : fed_conds_) {
                if (!cond.is_rhs_val && cond.rhs_col.tab_name != tab_name_) {
                    cond.is_rhs_val = true;
                    cond.rhs_val = all_dict_.at(cond.rhs_col)[l_cnt];
                }
                if(all_dict_.find(cond.rhs_col)==all_dict_.end()) { continue ;}
                cond.rhs_val = all_dict_.at(cond.rhs_col)[l_cnt];
            }
            for(;r_cnt<r_size_;r_cnt++){
                if(eval_conds(r_cols,fed_conds_,rhs_block_[r_cnt].get())){
                    return ;
                }
            }
            r_cnt=0;
        }
    }

    void nextTuple() override {

        for(;l_cnt<l_size_;l_cnt++){
            for (auto &cond : fed_conds_) {
                if (!cond.is_rhs_val && cond.rhs_col.tab_name != tab_name_) {
                    cond.is_rhs_val = true;
                    cond.rhs_val = all_dict_.at(cond.rhs_col)[l_cnt];
                }
                if(all_dict_.find(cond.rhs_col)==all_dict_.end()) { continue ;}
                cond.rhs_val = all_dict_.at(cond.rhs_col)[l_cnt];
            }
            for(;r_cnt<r_size_;r_cnt++){
                if(eval_conds(r_cols,fed_conds_,rhs_block_[r_cnt].get())){
                    return ;
                }
            }
            r_cnt = 0;
        }
    }
    bool is_end() const override { return (endend)||(l_cnt==lhs_block_.size());}
    bool has_nlj() override {
        return true;
    }
    std::string get_tbl_name() override {
        return left_->get_tbl_name();
    }
    size_t get_sort_offset() override {
        return l_len;
    }
    size_t tupleLen() const override { return len_; }

    std::vector<std::unique_ptr<RmRecord>> get_block() override {
        auto block = std::vector<std::unique_ptr<RmRecord>>{};
        while(!is_end()) {
            auto record = std::make_unique<RmRecord>(len_);
            memcpy(record->data, lhs_block_[l_cnt]->data, l_len);
            memcpy(record->data + l_len, rhs_block_[r_cnt]->data, r_len);
            r_cnt+=1;
            block.push_back(std::move(record));
            nextTuple();
        }
        return block;
    }

    std::unique_ptr<RmRecord> Next() override {
        if(is_end()) return nullptr;
        auto record = std::make_unique<RmRecord>(len_);
        memcpy(record->data, lhs_block_[l_cnt]->data, l_len);
        memcpy(record->data + l_len, rhs_block_[r_cnt]->data, r_len);
        r_cnt+=1;
        return record;
    }

    Rid &rid() override { return _abstract_rid; }
};