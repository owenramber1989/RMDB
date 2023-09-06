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
// #include "analyze/analyze.h"

class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<ColMeta> cols_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

public:
    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_; }
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
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
    bool check_index_match(std::vector<std::string>& col_names,std::vector<ColMeta>& cols){
        std::vector<std::string> cols_to_use;
        // 只要列被索引包含，匹配成功
        for(auto &col:cols){
            if (std::find_if(col_names.begin(),col_names.end(),[&](std::string& name){return name==col.name;})==col_names.end()){
                continue;
            }
            else{
                cols_to_use.push_back(col.name);
            }
        }
        return !cols_to_use.empty();
    }
    // 这个函数必须给它拆了
    void update_set(const std::string &tab_name, std::vector<SetClause> set_clauses) {
        auto txn = context_->txn_;
        /* 下面这段之前为什么留着？谁留的?
        TabMeta &tab = sm_manager_->db_.get_table(tab_name);
        for (auto &set_clause : set_clauses) {
            auto lhs_col = tab.get_col(set_clause.lhs.col_name);
            if (lhs_col->type != set_clause.rhs.type) {
                if((lhs_col->type == TYPE_BIGINT)&&(set_clause.rhs.type==TYPE_INT)){
                    set_clause.rhs.set_bigint(set_clause.rhs.int_val);
                } else {
                    throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(set_clause.rhs.type));
                }
            }
            set_clause.rhs.init_raw(lhs_col->len);
        }
         */
        // 记录set中用到的列名
        TabMeta &tab = sm_manager_->db_.get_table(tab_name);
        std::vector<std::string> col_names;
        // Get raw values in set clause
        for (auto &set_clause : set_clauses) {
            auto lhs_col = tab.get_col(set_clause.lhs.col_name);
            if (lhs_col->type != set_clause.rhs.type) {
                if(lhs_col->type==TYPE_STRING&&set_clause.rhs.type==TYPE_DATETIME&&lhs_col->len>=19) {
                } else if((lhs_col->type == TYPE_BIGINT)&&(set_clause.rhs.type==TYPE_INT)){
                    set_clause.rhs.set_bigint(set_clause.rhs.int_val);
                } else {
                    throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(set_clause.rhs.type));
                }
            }
            if(!set_clause.exp) {
                set_clause.rhs.init_raw(lhs_col->len);
            }
            col_names.emplace_back(set_clause.lhs.col_name);
        }
        // Get record file
        auto fh = sm_manager_->fhs_.at(tab_name).get();
        // Get all necessary index files
        auto ix_manager=sm_manager_->get_ix_manager();
        std::vector<IxIndexHandle*> ihs(tab.indexes.size(), nullptr);
        for (size_t index_i = 0; index_i < tab.indexes.size(); index_i++) {
            auto cols=tab.indexes[index_i].cols;
            if (!cols.empty()) {
                // match
                if (check_index_match(col_names,cols)){
                    ihs[index_i] = sm_manager_->ihs_.at(ix_manager->get_index_name(tab_name_, cols)).get();
                }
            }
        }
        // 计算给定name在索引列名中的位置
        auto index_pos=[](const std::vector<std::string>& index_col_names,const std::string&name)->int {
            for (auto i = 0; i < index_col_names.size(); i++) {
                if (index_col_names.at(i) == name) {
                    return i;
                }
            }
            return -1;
        };
        auto offset=[](const std::vector<int>&col_lens,int pos)->int{
            int off=0;
            for(int i=0;i<pos;i++){
                off+=col_lens[i];
            }
            return off;
        };
        // 对每一个索引进行唯一性检查
        // 保证新key和旧key不冲突，新key之间也不冲突
        bool unique=true;
        for(size_t index_i=0;index_i<tab.indexes.size();index_i++){
            if (ihs[index_i] != nullptr){
                // std::cout<<"In the uniqueness check phase, ihs[index_i] is not nullptr\n";
                std::vector<std::string> new_keys;
                size_t key_len;
                for(auto& rid:rids_){
                    auto rec = fh->get_record(rid,context_);
                    key_len=tab.indexes[index_i].col_tot_len;
                    char* key = new char[key_len+1];
                    memset(key, 0, key_len+1);
                    std::vector<std::string> index_col_names;
                    std::vector<int>col_len;
                    std::vector<int>col_off;
                    for (auto& col:tab.indexes[index_i].cols){
                        index_col_names.emplace_back(col.name);
                        col_len.emplace_back(col.len);
                        col_off.emplace_back(col.offset);
                    }
                    // make new key
                    for (auto& set_clause:set_clauses){
                        auto pos=index_pos(index_col_names,set_clause.lhs.col_name);
                        if (pos==-1)continue;
                        auto off = offset(col_len, pos);
                        if(set_clause.exp) {
                            auto data = rec->data + col_off[pos];
                            auto type = set_clause.rhs.type;
                            switch(type) {
                                case TYPE_INT:
                                    if(set_clause.add) {
                                        *(int *) data += set_clause.rhs.int_val;
                                    } else {
                                        *(int *) data -= set_clause.rhs.int_val;
                                    }
                                    break;
                                case TYPE_FLOAT:
                                    if(set_clause.add) {
                                        *(float*)data += set_clause.rhs.float_val;
                                    } else {
                                        *(float*)data -= set_clause.rhs.float_val;
                                    }
                                    break;
                                default:
                                    break;
                            }
                            memcpy(key + off, rec->data + col_off[pos], col_len[pos]);
                        } else {
                            memcpy(key + off, set_clause.rhs.raw->data, col_len[pos]);
                        }
                    }
                    // gain data from record
                    int count = 0;
                    for(auto& col:tab.indexes[index_i].cols){
                        // static int count=0;
                        // col_names就是set_clauses的左值列集合
                        if (std::find_if(col_names.begin(),col_names.end(),[&](const std::string& name){return name==col.name;})==col_names.end()){
                            auto col_needed = tab.get_col(col.name);
                            auto off=offset(col_len,count);
                            memcpy(key+off,rec->data+col_needed->offset,col_needed->len);
                        }
                        ++count;
                    }
                    std::vector<Rid> tmp;
                    // 去掉这一段会有重复，不去掉又不能再次更新刚才被abort de value
                    if(ihs[index_i]->get_value(key,&tmp,context_->txn_)) {
                        throw InternalError("uniqueness check failed1");
                    }
                    new_keys.emplace_back(key);
                    delete [] key;
                }
                for(auto i=0;i<new_keys.size();i++){
                    for(auto j=i+1;j<new_keys.size();j++){
                        if (memcmp(new_keys[i].c_str(),new_keys[j].c_str(),key_len)==0){
                            throw InternalError("uniqueness check failed2");
                        }
                    }
                }
            }
        }
        // Update each rid from record file and index file
        for (auto &rid : rids_){
            std::vector<char*> keys_to_delete(tab.indexes.size());
            std::vector<char*> keys_to_insert(tab.indexes.size());
            auto rec = fh->get_record(rid,context_);
            // RmRecord old_rec(rec->size,rec->data);
            // Remove old entry from index
            for (size_t index_i = 0; index_i < tab.indexes.size(); index_i++) {
                if (ihs[index_i] != nullptr) {
                    // std::cout<<"ihs[index_i] != nullptr\n";
                    char* key=new char[tab.indexes[index_i].col_tot_len];
                    memset(key, 0, tab.indexes[index_i].col_tot_len);
                    std::vector<std::string> index_col_names;
                    std::vector<int>col_len(tab.indexes[index_i].col_num);
                    for (auto& col:tab.indexes[index_i].cols){
                        index_col_names.emplace_back(col.name);
                    }
                    // make origin key
                    int off1 = 0;
                    for(auto& col:tab.indexes[index_i].cols){
                        // static int off=0;
                        auto col_needed = tab.get_col(col.name);
                        memcpy(key+off1,rec->data+col_needed->offset,col_needed->len);
                        off1+=col_needed->len;
                    }
                        LogRecord index_delete_log_record = LogRecord(context_->txn_->get_transaction_id(),
                                                                      context_->txn_->get_prev_lsn(),
                                                                      LogType::DELETE_ENTRY, rid, key,
                                                                      tab.indexes[index_i].col_tot_len,
                                                                      ix_manager->get_index_name(tab_name_,
                                                                                                 tab.indexes[index_i].cols));
                        context_->txn_->set_prev_lsn(context_->log_mgr_->add_log_to_buffer(&index_delete_log_record));
                    keys_to_delete[index_i]=key;
//                    auto flag = ihs[index_i]->delete_entry(key,context_->txn_);
//                    if(flag && context_->txn_->get_txn_mode()) {
//                        auto *indexWriteRecord = new IndexWriteRecord(WType::DELETE_TUPLE,
//                                                                                  tab_name_, rid, index_i,
//                                                                                  key,tab.indexes[index_i].col_tot_len);
//                        context_->txn_->append_index_write_record(indexWriteRecord);
//                    }
                }
            }

            // Update record in record file
            RmRecord old_rec(rec->size,rec->data);
            for (auto &set_clause : set_clauses) {
                auto lhs_col = tab.get_col(set_clause.lhs.col_name);
                if(set_clause.exp) {
                    auto data = rec->data + lhs_col->offset;
                    auto type = set_clause.rhs.type;
                    switch(type) {
                        case TYPE_INT:
                            if(set_clause.add) {
                                *(int *) data += set_clause.rhs.int_val;
                            } else {
                                *(int *) data -= set_clause.rhs.int_val;
                            }
                            break;
                        case TYPE_FLOAT:
                            if(set_clause.add) {
                                *(float*)data += set_clause.rhs.float_val;
                            } else {
                                *(float*)data -= set_clause.rhs.float_val;
                            }
                            break;
                        default:
                            break;
                    }
                } else {
                    memcpy(rec->data + lhs_col->offset, set_clause.rhs.raw->data, lhs_col->len);
                }
            }
            RmRecord new_rec(rec->size,rec->data);
                LogRecord updateLogRecord(context_->txn_->get_transaction_id(), context_->txn_->get_prev_lsn(),
                                          LogType::UPDATE, rid, old_rec, new_rec, tab_name_);
                txn->set_prev_lsn(context_->log_mgr_->add_log_to_buffer(&updateLogRecord));
            // Insert new entry into index
            for (size_t index_i = 0; index_i < tab.indexes.size(); index_i++) {
                if (ihs[index_i] != nullptr) {
                    char* key=new char[tab.indexes[index_i].col_tot_len];
                    memset(key, 0, tab.indexes[index_i].col_tot_len);
                    std::vector<std::string> index_col_names;
                    std::vector<int>col_len(tab.indexes[index_i].col_num);
                    for (auto& col:tab.indexes[index_i].cols){
                        index_col_names.emplace_back(col.name);
                        col_len.emplace_back(col.len);
                    }
                    // make new key
                    int off2 = 0;
                    for(auto& col:tab.indexes[index_i].cols){
                        auto col_needed = tab.get_col(col.name);
                        memcpy(key+off2,rec->data+col_needed->offset,col_needed->len);
                        off2+=col_needed->len;
                    }
                        LogRecord index_insert_log_record = LogRecord(txn->get_transaction_id(), txn->get_prev_lsn(),
                                                                      LogType::INSERT_ENTRY, rid,
                                                                      key, tab.indexes[index_i].col_tot_len,
                                                                      sm_manager_->get_ix_manager()->get_index_name(
                                                                              tab_name_, tab.indexes[index_i].cols));
                        txn->set_prev_lsn(context_->log_mgr_->add_log_to_buffer(&index_insert_log_record));
                    keys_to_insert[index_i]=key;
//                    auto flag = ihs[index_i]->insert_entry(key,rid,context_->txn_);
//                    if(flag && context_->txn_->get_txn_mode()) {
//                        auto *indexWriteRecord = new IndexWriteRecord(WType::INSERT_TUPLE,
//                                                                                  tab_name_, index_i, key,tab.indexes[index_i].col_tot_len);
//                        // std::cout<<"update insert entry"<<std::endl;
//                        txn->append_index_write_record(indexWriteRecord);
//                    }
                }
            }
            // update
            fh->update_record(rid, rec->data,context_);
            if(txn->get_txn_mode()) {
                auto *writeRecord = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid,old_rec,new_rec);
                txn->append_write_record(writeRecord);
            }
            for(size_t index_i = 0; index_i < tab.indexes.size(); index_i++){
                if (ihs[index_i] != nullptr){
                    // delete from index
                    auto flag = ihs[index_i]->delete_entry(keys_to_delete[index_i],context_->txn_);
                    if(flag && context_->txn_->get_txn_mode()) {
                        auto *indexWriteRecord = new IndexWriteRecord(WType::DELETE_TUPLE,
                                                                                  tab_name_, rid, index_i,
                                                                                  keys_to_delete[index_i],tab.indexes[index_i].col_tot_len);
                        context_->txn_->append_index_write_record(indexWriteRecord);
                    } else {
                        delete [] keys_to_delete[index_i];
                    }
                    // insert into index
                    flag = ihs[index_i]->insert_entry(keys_to_insert[index_i],rid,context_->txn_);
                    if(flag && context_->txn_->get_txn_mode()) {
                        auto *indexWriteRecord = new IndexWriteRecord(WType::INSERT_TUPLE,
                                                                                  tab_name_, index_i, keys_to_insert[index_i],tab.indexes[index_i].col_tot_len);
                        txn->append_index_write_record(indexWriteRecord);
                    } else{
                        delete[] keys_to_insert[index_i];
                    }
                }
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (rids_.empty()) {
            return nullptr;
        }
        if(context_->txn_->get_txn_mode()) {
            auto tab_fd = fh_->GetFd();
            // std::cout<<"in update executor: the fd is "<<tab_fd<<"\n";
            auto lock_mgr = context_->lock_mgr_;
            if (!lock_mgr->lock_on_table(context_->txn_, tab_fd, LockMode::EXCLUSIVE)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::FAILED_TO_LOCK);
            }
        }
        update_set(tab_name_,set_clauses_);
        return nullptr;
    }
    void feed(const std::map<TabCol, Value> &feed_dict) override {
        throw InternalError("Cannot feed a projection node");
    }

    Rid &rid() override { return _abstract_rid; }
};