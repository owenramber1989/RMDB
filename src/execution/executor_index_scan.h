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
#include <memory>
#include <string.h>
class IndexScanExecutor : public AbstractExecutor {
private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

public:
    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_; }
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                      Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
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
    }
    static std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }
    std::string get_tbl_name() override {
        return tab_name_;
    }
    size_t tupleLen() const override { return len_; }
    TabMeta get_tables() override {
        return sm_manager_->db_.get_table(tab_name_);
    }
    void check_runtime_conds() {
        for (auto &cond: fed_conds_) {
            assert(cond.lhs_col.tab_name == tab_name_);
        }
    }
    void feed(const std::map<TabCol, Value> &feed_dict) {
        for (auto &cond : fed_conds_) {
            if (!cond.is_rhs_val && cond.rhs_col.tab_name != tab_name_) {
                cond.is_rhs_val = true;
                cond.rhs_val = feed_dict.at(cond.rhs_col);
            }
            if(feed_dict.find(cond.rhs_col)==feed_dict.end()) { continue ;}
            cond.rhs_val = feed_dict.at(cond.rhs_col);
        }
        check_runtime_conds();
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
                block.push_back(std::move(rec));
            }
            scan_->next();
        }
        return block;
    }
    bool eval_cond(const std::vector<ColMeta> &rec_cols,
                   const Condition &cond,
                   const RmRecord *rec) {
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        auto *lhs = rec->data + lhs_col->offset;
        char* rhs;
//        std::cout<<"lhs="<<*(int*)lhs<<std::endl;
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
        } // TODO convert to common type
        int cmp = ix_compare(lhs, rhs, rhs_type, lhs_col->len);
        if (cond.op == OP_EQ) {
//            std::cout<<"op==EQ ,cmp=="<<cmp<<std::endl;
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

    void make_key(const std::vector<Condition>& conds,const std::vector<std::string>& index_col_names,std::vector<ColMeta>& cols,std::vector<int>& lens,char* key) {
        auto index_pos = [&index_col_names](const std::string &name) -> int {
            for (auto i = 0; i < index_col_names.size(); i++) {
                if (index_col_names.at(i) == name) {
                    return i;
                }
            }
            return -1;
        };
        auto offset = [](const std::vector<int> &tmp, int pos) -> int {
            int off = 0;
            for (int i = 0; i < pos; ++i) {
                off += tmp[i];
            }
            return off;
        };
        for (auto &cond: conds) {
            if (cond.is_rhs_val && (cond.op == OP_EQ || cond.op == OP_GT)) {
                switch (cond.rhs_val.type) {
                    case TYPE_STRING: {
                        auto tmp = cond.rhs_val.str_val.c_str();
                        auto pos = index_pos(cond.lhs_col.col_name);
                        if (pos == -1)break;
                        auto off = offset(lens, pos);
                        memcpy(key + off, tmp, lens[pos]);
                        break;
                    }

                    case TYPE_INT: {
                        auto tmp = (const char *) (&cond.rhs_val.int_val);
                        auto pos = index_pos(cond.lhs_col.col_name);
                        if (pos == -1)break;
                        auto off = offset(lens, pos);
                        memcpy(key + off, tmp, lens[pos]);
                        break;
                    }
                    case TYPE_FLOAT: {
                        auto tmp = (const char *) (&cond.rhs_val.float_val);
                        auto pos = index_pos(cond.lhs_col.col_name);
                        auto off = offset(lens, pos);
                        if (pos == -1)break;
                        memcpy(key + off, tmp, lens[pos]);
                        break;
                    }
                    case TYPE_BIGINT: {
                        auto tmp = (const char *) (&cond.rhs_val.bigint_val);
                        auto pos = index_pos(cond.lhs_col.col_name);
                        if (pos == -1)break;
                        auto off = offset(lens, pos);
                        memcpy(key + off, tmp, lens[pos]);
                        break;
                    }
                    case TYPE_DATETIME: {
                        auto tmp = cond.rhs_val.str_val.c_str();
                        auto pos = index_pos(cond.lhs_col.col_name);
                        if (pos == -1)break;
                        auto off = offset(lens, pos);
                        memcpy(key + off, tmp, lens[pos]);
                        break;
                    }
                }
            }
        }
    }
    void beginTuple() override {
        if(context_->txn_->get_txn_mode()) {
            auto tab_fd = fh_->GetFd();
            auto lock_mgr = context_->lock_mgr_;
            if (!lock_mgr->lock_on_table(context_->txn_, tab_fd,LockMode::SHARED)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::FAILED_TO_LOCK);
            }
//            std::cout<<"txn #"<<context_->txn_->get_transaction_id()<< " successfully get the seq scan s lock\n";
        }
        check_runtime_conds();
        // std::cout<<"beginTuple---使用索引查询"<<std::endl;
        auto ix_manager=sm_manager_->get_ix_manager();
        auto index_name=ix_manager->get_index_name(tab_name_,index_col_names_);
        if (sm_manager_->ihs_.find(index_name)==sm_manager_->ihs_.end()){
            sm_manager_->ihs_[index_name]=ix_manager->open_index(tab_name_,index_col_names_);
        }
        // 构造key
        std::vector<int>col_len(index_meta_.col_num);
        int count=0;
        for(auto& col:index_meta_.cols){
            col_len[count++]=col.len;
        }
        int key_len=index_meta_.col_tot_len;
        char key[key_len];
        memset(key,0,key_len);
        assert(!index_col_names_.empty());
        make_key(fed_conds_,index_col_names_,cols_,col_len,key);
//        std::cout<<"e\n";
        int id=*(int*)(key);
        // 获取迭代范围
        auto ih=sm_manager_->ihs_[index_name].get();
        assert(ih!= nullptr);
//        std::cout<<"num pages="<<ih->get_ix_file_hdr()->num_pages_<<"\n";
        auto start=ih->leaf_begin(key);
        auto end=ih->leaf_end();
//        std::cout<<"start.page no="<<start.page_no<<", slot_no="<<start.slot_no<<"\n";
//        std::cout<<"end.page no="<<end.page_no<<", slot_no="<<end.slot_no<<"\n";
        scan_=std::make_unique<IxScan>(ih,start,end,sm_manager_->get_bpm());
        assert(scan_!= nullptr);
        while (!scan_->is_end()){
            rid_=scan_->rid();
            auto rec=fh_->get_record(rid_,context_);
            if (eval_conds(cols_,fed_conds_,rec.get())){
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
        scan_->next();
        // 只要开始不满足条件，后续所有的元组都不满足条件，直接置为end
        if (!scan_->is_end()){
            auto rec=fh_->get_record(scan_->rid(),context_);
            if (!eval_conds(cols_,fed_conds_,rec.get())){
                scan_->set_end();
            }else{
                rid_=scan_->rid();
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
//        std::cout<<"Next() of scan is called"<<std::endl;
        if(is_end()){
            return nullptr;
        }
        return fh_->get_record(rid_,context_);
    }

    bool is_end() const override {
        return scan_->is_end();
    }
    Rid &rid() override { return rid_; }

};
