/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

#include <shared_mutex>
#include "recovery/log_manager.h"

std::unordered_map<txn_id_t, std::shared_ptr<Transaction>> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
std::shared_ptr<Transaction> TransactionManager::begin(std::shared_ptr<Transaction> txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    if (txn == nullptr) {
        txn = std::make_shared<Transaction>(next_txn_id_++); // memory leak
    }
    LogRecord log(txn->get_transaction_id(), txn->get_prev_lsn(), LogType::BEGIN);
    txn->set_prev_lsn(log_manager->add_log_to_buffer(&log));
    txn->set_state(TransactionState::DEFAULT);
    std::unique_lock<std::mutex> lock(latch_);
    txn_map[txn->get_transaction_id()] = txn;
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(std::shared_ptr<Transaction> txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if(!txn->get_txn_mode()) {
        LogRecord log(txn->get_transaction_id(), txn->get_prev_lsn(), LogType::COMMIT);
        txn->set_prev_lsn(log_manager->add_log_to_buffer(&log));
        txn->set_state(TransactionState::COMMITTED);
        return;
    }
    auto write_set = txn->get_write_set();
    auto index_write_set = txn->get_index_write_set();
    for(auto& t:*write_set){
        delete t;
    }
    for(auto& t:*index_write_set){
        delete t;
    }
    write_set->clear();
    index_write_set->clear();
    // TODO: write log and update transaction's prev_lsn here
    LogRecord log(txn->get_transaction_id(), txn->get_prev_lsn(), LogType::COMMIT);
    txn->set_prev_lsn(log_manager->add_log_to_buffer(&log));
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(std::shared_ptr<Transaction> txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if(!txn->get_txn_mode()) {
        return;
    }
//     std::cout<<"显式事务abort"<<std::endl;
    auto table_write_records = txn->get_write_set();
    while (!table_write_records->empty()) {
        auto record = table_write_records->back();
        table_write_records->pop_back();
        auto write_type = record->GetWriteType();
        auto tab_name_ = record->GetTableName();
        auto rid = record->GetRid();
        auto fh = sm_manager_->fhs_.at(tab_name_).get();
        Context* context_ = nullptr;
        if (write_type == WType::INSERT_TUPLE) {
            auto deleteLogRecord = LogRecord(txn->get_transaction_id(),txn->get_prev_lsn(),LogType::DELETE,rid,record->GetRecord(),tab_name_);
            txn->set_prev_lsn(log_manager->add_log_to_buffer(&deleteLogRecord));
            fh->delete_record(rid,context_);
//            std::cout<<"insert abort end\n";
        } else if (write_type == WType::DELETE_TUPLE) {
            // std::cout<<"delete abort\n";
            auto rec = record->GetRecord();
            auto insertLogRecord = LogRecord(txn->get_transaction_id(),txn->get_prev_lsn(),LogType::INSERT,rid,rec,tab_name_);
            txn->set_prev_lsn(log_manager->add_log_to_buffer(&insertLogRecord));
            fh->insert_record(rid,rec.data);
//             std::cout<<"delete abort end\n";
        } else if (write_type == WType::UPDATE_TUPLE){
            // std::cout<<"update abort\n";
            auto rec = record->GetRecord();
            auto new_rec = record->GetNewRec();
            if(!fh->is_record(rid)) {
                continue;
            }
            // auto old_rec = fh->get_record(rid,context_);//得到文件中旧的记录
            LogRecord updateLogRecord(txn->get_transaction_id(),txn->get_prev_lsn(),LogType::UPDATE, rid, new_rec, rec, tab_name_);
            txn->set_prev_lsn(log_manager->add_log_to_buffer(&updateLogRecord));
            fh->update_record(rid,rec.data,context_);
//             std::cout<<"update abort end\n";
        } delete record;
    }
    table_write_records->clear();

    auto index_write_set = txn->get_index_write_set();

    while (!index_write_set->empty()) {
        auto &item = index_write_set->back();
        if (item->GetWriteType() == WType::DELETE_TUPLE) {
//             std::cout<<"in index delete abort\n";
            auto tab_name_ = item->GetTableName();
            auto index = sm_manager_->db_.get_table(item->GetTableName()).indexes[item->GetIx()];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            int len = item->GetLen();
            char* orig_key = new char[len];
            std::memcpy(orig_key, item->GetKey(), len);
            LogRecord index_insert_log_record=LogRecord(txn->get_transaction_id(),txn->get_prev_lsn(),LogType::INSERT_ENTRY,item->GetRid(),orig_key,index.col_tot_len,sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols));
            txn->set_prev_lsn(log_manager->add_log_to_buffer(&index_insert_log_record));
            ih->insert_entry(orig_key,item->GetRid(),txn);
            delete [] orig_key;
           // std::cout<<"in index abort insert: the lsn is "<<index_insert_log_record.GetLSN()<<"\n";
           // std::cout<<"index delete abort end\n";
        } else if (item->GetWriteType() == WType::INSERT_TUPLE) {
//             std::cout<<"in index insert abort\n";
            auto tab_name_ = item->GetTableName();
            auto index = sm_manager_->db_.get_table(item->GetTableName()).indexes[item->GetIx()];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            int len = item->GetLen();
            char* orig_key = new char[len];
            std::memcpy(orig_key, item->GetKey(), len);
            LogRecord index_delete_log_record=LogRecord(txn->get_transaction_id(),txn->get_prev_lsn(),
                                                        LogType::DELETE_ENTRY,item->GetRid(),orig_key,index.col_tot_len,
                                                        sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols));
            txn->set_prev_lsn(log_manager->add_log_to_buffer(&index_delete_log_record));
            ih->delete_entry(orig_key,txn);
            delete [] orig_key;
            // std::cout<<"in index abort insert: the lsn is "<<index_delete_log_record.GetLSN()<<"\n";
            // std::cout<<"index insert abort end\n";
        }
        index_write_set->pop_back();
        delete item;
    }
    index_write_set->clear();
        LogRecord log(txn->get_transaction_id(), txn->get_prev_lsn(), LogType::ABORT);
        txn->set_prev_lsn(log_manager->add_log_to_buffer(&log));
    txn->set_state(TransactionState::ABORTED);
}

