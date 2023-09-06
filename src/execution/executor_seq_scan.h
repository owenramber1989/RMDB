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
#include "analyze/analyze.h"
#include "system/sm.h"

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同
    static std::map<CompOp, CompOp> swap_op;
    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        // std::cout<<"seq scan cols() is called"<<std::endl;
        return cols_; }


        SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);

        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

            static std::map<CompOp, CompOp> swap_op = {
                    {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
            };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
        /*
        for(auto cond : fed_conds_) {
            std::cout<<cond.to_string()<<std::endl;
        }
         */
    }
    std::string get_tbl_name() override {
        return tab_name_;
    }
    TabMeta get_tables() override {
        return sm_manager_->db_.get_table(tab_name_);
    }
    void check_runtime_conds() {
        for (auto &cond: fed_conds_) {
            assert(cond.lhs_col.tab_name == tab_name_);
            if (!cond.is_rhs_val) {
                // std::cout<<tab_name_<<"\n"<<cond.rhs_col.tab_name<<std::endl;
                assert(cond.rhs_col.tab_name == tab_name_);
            }
        }
    }

    void fast_feed(const std::map<TabCol, std::vector<Value>> &feed_dict, size_t cnt) override {
        for (auto &cond : fed_conds_) {
            if (!cond.is_rhs_val && cond.rhs_col.tab_name != tab_name_) {
                cond.is_rhs_val = true;
                cond.rhs_val = feed_dict.at(cond.rhs_col)[cnt];
            }
            if(feed_dict.find(cond.rhs_col)==feed_dict.end()) { continue ;}
            cond.rhs_val = feed_dict.at(cond.rhs_col)[cnt];
        }
        check_runtime_conds();
    }
    void set_conds(std::vector<Condition> conds) override {
        static std::map<CompOp, CompOp> swap_op_ack = {
                {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };
        for (auto &cond : conds) {
             if(cond.lhs_col.tab_name != tab_name_) {
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op_ack.at(cond.op);
            }
        }
        fed_conds_ = conds;
    }
    size_t tupleLen() const override { return len_; }
    static std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    bool is_end() const override {
        return scan_->is_end();
    }


    void feed(const std::map<TabCol, Value> &feed_dict) {
        // fed_conds_ = conds_;
        for (auto &cond : fed_conds_) {
            // 如果右侧的表名和当前的表名不一样，就说明是在nlj阶段，把lhs_tbl.attr1 < rhs_tbl.attr2
            // 换成lhs_tbl.attr1 < concrete value
            if (!cond.is_rhs_val && cond.rhs_col.tab_name != tab_name_) {
                cond.is_rhs_val = true;
                cond.rhs_val = feed_dict.at(cond.rhs_col);
            }
            // std::cout<<"feed\n\n"<<cond.to_string()<<std::endl;
            // 没有下面这一行就没法做双表列判断，有了下面这一行就没法做单表列判断
            // 之所以要加下面这一行才能做nlj，是因为每一次更新left的时候，都必须feedright to get new value, but
            // at this very moment, the right is not a column, but a value, so failed to update
            if(feed_dict.find(cond.rhs_col)==feed_dict.end()) { continue ;}
            cond.rhs_val = feed_dict.at(cond.rhs_col);
        }
        check_runtime_conds();
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
            // 这里暂时没有管长度
            if(rhs_type==TYPE_DATETIME&&lhs_col->type==TYPE_STRING&&lhs_col->len>=19) {
            } else if(rhs_type==TYPE_INT&&lhs_col->type==TYPE_BIGINT){}else {
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

    std::vector<std::unique_ptr<RmRecord>> get_block() override {
        if(context_->txn_->get_txn_mode()) {
            auto tab_fd = fh_->GetFd();
            auto lock_mgr = context_->lock_mgr_;
            if (!lock_mgr->lock_on_table(context_->txn_, tab_fd, LockMode::SHARED)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::FAILED_TO_LOCK);
            }
        }
        auto block = std::vector<std::unique_ptr<RmRecord>>{};
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_,context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                // rec2dict(cols_,rec.get());
                block.push_back(std::move(rec));
            }
            scan_->next();
        }
        return block;
    }

    bool eval_conds(const std::vector<ColMeta> &rec_cols,
                           const std::vector<Condition> &conds,
                           const RmRecord *rec) {
        return std::all_of(conds.begin(), conds.end(), [&](const Condition &cond) {
            return eval_cond(rec_cols, cond, rec);
        });
    }

    void beginTuple() override {
        // std::cout<<"seq scan begin tuple"<<std::endl;
        if(context_->txn_->get_txn_mode()) {
            auto tab_fd = fh_->GetFd();
            auto lock_mgr = context_->lock_mgr_;
            if (!lock_mgr->lock_on_table(context_->txn_, tab_fd, LockMode::SHARED)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::FAILED_TO_LOCK);
            }
        }
        check_runtime_conds();
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_,context_);

            if (eval_conds(cols_, fed_conds_, rec.get())) {
                break;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        check_runtime_conds();
        if(is_end()){
            return;
        }
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_,context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                break;
            }
        }
    }

    ColMeta get_col_offset(const TabCol &target) override {
            for (auto &col : cols_) {
                if (col.name == target.col_name) {
                    return col;
                }
            }
        throw InternalError("The column to be ordered by does not exist.");
    }
    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_,context_);
    }

    Rid &rid() override { return rid_; }
};