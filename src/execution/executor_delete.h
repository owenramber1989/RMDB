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


class DeleteExecutor : public AbstractExecutor {
private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

public:
    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        // return cols_;
    }
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::vector<ColMeta> get_all_cols(const std::vector<std::string> &tab_names) {
        std::vector<ColMeta> all_cols;
        for (auto &sel_tab_name : tab_names) {
            const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
            all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
        }
        return all_cols;
    }

    TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
        if (target.tab_name.empty()) {
            // Table name not specified, infer table name from column name
            std::string tab_name;
            for (auto &col : all_cols) {
                if (col.name == target.col_name) {
                    if (!tab_name.empty()) {
                        throw AmbiguousColumnError(target.col_name);
                    }
                    tab_name = col.tab_name;
                }
            }
            if (tab_name.empty()) {
                throw ColumnNotFoundError(target.col_name);
            }
            target.tab_name = tab_name;
        } else {
            // Make sure target column exists
            if (!(sm_manager_->db_.is_table(target.tab_name) &&
                  sm_manager_->db_.get_table(target.tab_name).is_col(target.col_name))) {
                throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
            }
        }
        return target;
    }
    std::vector<Condition> check_where_clause(const std::vector<std::string> &tab_names,
                                              const std::vector<Condition> &conds) {
        auto all_cols = get_all_cols(tab_names);
        // Get raw values in where clause
        std::vector<Condition> res_conds = conds;
        for (auto &cond : res_conds) {
            // Infer table name from column name
            cond.lhs_col = check_column(all_cols, cond.lhs_col);
            if (!cond.is_rhs_val) {
                cond.rhs_col = check_column(all_cols, cond.rhs_col);
            }
            TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
            auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
            ColType lhs_type = lhs_col->type;
            ColType rhs_type;
            if (cond.is_rhs_val) {
                cond.rhs_val.init_raw(lhs_col->len);
                rhs_type = cond.rhs_val.type;
            } else {
                TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
                auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
                rhs_type = rhs_col->type;
            }
            if (lhs_type != rhs_type) {
                if(lhs_type==TYPE_STRING&&rhs_type==TYPE_DATETIME&&lhs_col->len>=19){}
                else {
                    throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
                }
            }
        }
        return res_conds;
    }
    // update delete_from
    void delete_from(Rid& rid){
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        auto fh = sm_manager_->fhs_.at(tab_name_).get();
        auto ix_manager=sm_manager_->get_ix_manager();
        std::vector<IxIndexHandle *> ihs(tab.indexes.size(), nullptr);
        for (size_t index_i = 0; index_i < tab.indexes.size(); index_i++) {
            if (!tab.indexes[index_i].cols.empty()) {
                ihs[index_i] = sm_manager_->ihs_.at(ix_manager->get_index_name(tab_name_, tab.indexes[index_i].cols)).get();
            }
        }
        std::vector<char*> keys(tab.indexes.size());
        auto rec = fh->get_record(rid,context_);
        for (size_t index_i = 0; index_i < tab.indexes.size(); index_i++) {
            if (ihs[index_i] != nullptr) {
                char *key = new char[tab.indexes[index_i].col_tot_len];
                memset(key, 0, tab.indexes[index_i].col_tot_len);
                int offset = 0;
                for (auto &col: tab.indexes[index_i].cols) {
                    memcpy(key + offset, rec->data + col.offset, col.len);
                    offset += col.len;
                }
                    LogRecord index_delete_log_record = LogRecord(context_->txn_->get_transaction_id(),
                                                                  context_->txn_->get_prev_lsn(),
                                                                  LogType::DELETE_ENTRY, rid, key,
                                                                  tab.indexes[index_i].col_tot_len,
                                                                  ix_manager->get_index_name(tab_name_,
                                                                                             tab.indexes[index_i].cols));
                    context_->txn_->set_prev_lsn(context_->log_mgr_->add_log_to_buffer(&index_delete_log_record));
                keys[index_i]=key;
            }
        }
        for(size_t index_i = 0; index_i < tab.indexes.size(); index_i++){
            if (ihs[index_i] != nullptr){
                auto flag = ihs[index_i]->delete_entry(keys[index_i], context_->txn_);
                if(flag&&context_->txn_->get_txn_mode()) {
                    auto *indexWriteRecord = new IndexWriteRecord(WType::DELETE_TUPLE, tab_name_,
                                                                  rid, index_i, keys[index_i],tab.indexes[index_i].col_tot_len);
                    context_->txn_->append_index_write_record(indexWriteRecord);
                }
            }
        }
    }


    std::unique_ptr<RmRecord> Next() override {
        auto txn = context_->txn_;
        if(txn->get_txn_mode()) {
            auto tab_fd = fh_->GetFd();
            auto lock_mgr = context_->lock_mgr_;
            if (!lock_mgr->lock_on_table(context_->txn_, tab_fd,LockMode::EXCLUSIVE)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::FAILED_TO_LOCK);
            }
            // std::cout<<"txn #"<<context_->txn_->get_transaction_id()<< " successfully get the delete x lock\n";
        }
        for(auto rid : rids_) {
            auto rec = fh_->get_record(rid, context_);
            RmRecord rmRecord(rec->size,rec->data);
                auto deleteLogRecord = LogRecord(context_->txn_->get_transaction_id(), context_->txn_->get_prev_lsn(),
                                                 LogType::DELETE, rid, rmRecord, tab_name_);
                txn->set_prev_lsn(context_->log_mgr_->add_log_to_buffer(&deleteLogRecord));
            delete_from(rid);
            fh_->delete_record(rid,context_);
            if(txn->get_txn_mode()) {
                auto *writeRecord_insert = new WriteRecord(WType::INSERT_TUPLE, tab_name_,rid); // memory leak
                if(txn->exists_in_write_set(*writeRecord_insert)) {
                    // std::cout<<"delete executor: existed\n";
                    txn->remove_from_write_set(*writeRecord_insert);
                    continue;
                } else {
                    auto *writeRecord = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid,*rec);
                    txn->append_write_record(writeRecord);
                }
            }
        }
        return nullptr;
    }
    void feed(const std::map<TabCol, Value> &feed_dict) override {
        throw InternalError("Cannot feed a projection node");
    }
    Rid &rid() override { return _abstract_rid; }
};