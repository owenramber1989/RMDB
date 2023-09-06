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
#include "common/config.h"

class InsertExecutor : public AbstractExecutor {
private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    std::vector<ColMeta> cols_;
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

public:
    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_; }
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        if(context_->txn_->get_txn_mode()) {
            auto tab_fd = fh_->GetFd();
            auto lock_mgr = context_->lock_mgr_;
            if (!lock_mgr->lock_on_table(context_->txn_, tab_fd, LockMode::EXCLUSIVE)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::FAILED_TO_LOCK);
            }
            // std::cout<<"txn #"<<context_->txn_->get_transaction_id()<< " successfully get the insert x lock\n";
        }
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        // 把要插入的数据先全部存到rec
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type != val.type) {
                if(val.type==TYPE_DATETIME&&col.type==TYPE_STRING&&val.str_val.size()<=col.len) {
                } else if((col.type==TYPE_BIGINT)&&(val.type==TYPE_INT)) {
                    val.set_bigint(val.int_val);
                } else {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        // 唯一性检查
        std::vector<size_t> ins;
        for(size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto& index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char key[index.col_tot_len];
            memset(key,0,index.col_tot_len);
            int offset = 0;
            for(size_t i_ = 0; i_ < index.col_num; ++i_) {
                memcpy(key + offset, rec.data + index.cols[i_].offset, index.cols[i_].len);
                offset += index.cols[i_].len;
            }
            std::vector<Rid> value;
            bool unique=!ih->get_value(key,&value,context_->txn_);
            if (!unique){
                throw InternalError("failed the uniqueness check");
            }
            ins.push_back(i);
        }
        // Insert into record file
        rid_ = fh_->insert_record(rec.data, context_);
        auto txn = context_->txn_;
        if(txn->get_txn_mode()) {
            auto *writeRecord = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_, rec );
            txn->append_write_record(writeRecord);
        }
            auto insertLogRecord = LogRecord(txn->get_transaction_id(), txn->get_prev_lsn(), LogType::INSERT, rid_, rec,
                                             tab_name_);
            txn->set_prev_lsn(context_->log_mgr_->add_log_to_buffer(&insertLogRecord));
        std::vector<char*> keys(ins.size());
        // 唯一，就插入记录中
        for (auto j = 0; j<ins.size() ; ++j) {
            auto i=ins[j];
            auto& index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char *key = new char[index.col_tot_len];
            memset(key,0,index.col_tot_len);
            int offset = 0;
            for(size_t i_ = 0; i_ < index.col_num; ++i_) {
                memcpy(key + offset, rec.data + index.cols[i_].offset, index.cols[i_].len);
                offset += index.cols[i_].len;
            }
            // index log
                auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
                // ih->insert_entry(key, rid_, context_->txn_);
                LogRecord index_insert_log_record = LogRecord(txn->get_transaction_id(), txn->get_prev_lsn(),
                                                              LogType::INSERT_ENTRY, rid_, key, index.col_tot_len,
                                                              index_name);
                txn->set_prev_lsn(context_->log_mgr_->add_log_to_buffer(&index_insert_log_record));
            keys[j]=key;
        }
        for(int j=0;j<ins.size();++j){
            auto i=ins[j];
            auto& index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            auto flag = ih->insert_entry(keys[j], rid_, context_->txn_);
            if(flag&&context_->txn_->get_txn_mode()) {
                auto *indexWriteRecord = new IndexWriteRecord(WType::INSERT_TUPLE, tab_name_, i,
                                                              keys[j],index.col_tot_len);
                txn->append_index_write_record(indexWriteRecord);
            }
        }
        return nullptr;
    }
    void feed(const std::map<TabCol, Value> &feed_dict) override {
        throw InternalError("Cannot feed a projection node");
    }
    Rid &rid() override { return rid_; }
};
