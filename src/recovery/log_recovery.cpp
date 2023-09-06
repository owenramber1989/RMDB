/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

#include <chrono>
#include <thread>
#include <algorithm>

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {

}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    log_buffer_.offset_ = 0;
    int tail_length=0;
    int finished_length=0;
    while(disk_manager_->read_log(log_buffer_.buffer_, LOG_BUFFER_SIZE - tail_length, log_buffer_.offset_)>0)
    {
        // shall the ">0" attract some bug?
        // std::cout<<"1\n";
        LogRecord log;
        int buffer_offset_ = 0;
        tail_length=0;// 上次残留已经和读入的部分形成完整log buffer
        finished_length=0;// 上次残留已经被读入
        log.deserialize(log_buffer_.buffer_ + buffer_offset_);
        while(log.lsn_ != INVALID_LSN && log.log_tid_ != INVALID_TXN_ID &&log.GetLogRecordType()!=LogType::INVALID&&log.GetSize()>0)
        {
            // std::cout<<"the current log lsn is "<<log.GetLSN()<<"\n";
            // std::cout<<"2\n";
            lsn_mapping_[log.GetLSN()] = log_buffer_.offset_ + buffer_offset_;
//            std::cout<<"in redo the lsn is "<<log.GetLSN()<<"\n";
//            std::cout<<"in redo the type is "<<LogTypeStr[log.GetLogRecordType()]<<"\n";
            if(log.GetLogRecordType() == LogType::COMMIT ||log.GetLogRecordType() == LogType::ABORT)
                // if(log.GetLogRecordType() == LogType::COMMIT)
            {
                // std::cout<<"3\n";
                // 如果是commit或者是abort就从活动事务中删除
                active_txn_.erase(log.GetTxnId());
                auto it=std::find(active_txn_list_.begin(),active_txn_list_.end(),log.GetTxnId());

                if (it!=active_txn_list_.end()){
                    active_txn_list_.erase(it);
                }
            }
            else
            {
                // 添加进活动事务
                active_txn_[log.GetTxnId()] = log.GetLSN();
                // active_txn_.push_back(std::make_pair(log.GetTxnId(),log.GetLSN()));
                if (log.GetLogRecordType()==LogType::BEGIN){
                    active_txn_list_.emplace_back(log.GetTxnId());
                }

                // 分情况重做
                if(log.GetLogRecordType() == LogType::INSERT)
                {
                    log.deserialize_i_and_d(log_buffer_.buffer_ + buffer_offset_);
                    // TODO(zjm) use bpm and page to insert
                    auto tblname = log.get_table_name();
                    std::string file_name(tblname, log.get_table_name_size());
                    auto fh = sm_manager_->fhs_[file_name].get();
                    char* val = new char[log.GetValue().size+1];
                    memcpy(val,log.GetValue().data,log.GetValue().size);
                    try{
                        fh->insert_record(log.GetRid(),val);
                    } catch(RMDBError& e){
                        fh->insert_record(val,nullptr);
                    }
                    delete [] val;
                }
                else if(log.GetLogRecordType() == LogType::DELETE)
                {
                    log.deserialize_i_and_d(log_buffer_.buffer_+buffer_offset_);
                    Rid cur_rid = log.GetRid();
                    // TODO(zjm) use bpm and page to delete
                    auto tblname = log.get_table_name();
                    std::string file_name(tblname, log.get_table_name_size());
                        auto fh = sm_manager_->fhs_[file_name].get();
                        Context* context = nullptr;
                        fh->delete_record(cur_rid,context);
                }
                else if(log.GetLogRecordType() == LogType::UPDATE)
                {
                    log.deserialize_upd(log_buffer_.buffer_+buffer_offset_);
                    Rid cur_rid = log.GetRid();
                    // TODO(zjm) use bpm and page to update
                    auto tblname = log.get_table_name();
                    std::string file_name(tblname, log.get_table_name_size());
                        auto fh = sm_manager_->fhs_[file_name].get();
                        // auto fh = std::move(sm_manager_->fhs_[file_name]);
                        char* val = new char[log.GetNewValue().size+1];
                        memcpy(val,log.GetNewValue().data,log.GetNewValue().size);
                        Context* context = nullptr;
                        fh->update_record(cur_rid,val,context);
                        delete [] val;
                    // buffer_pool_manager_->unpin_page(cur_pageId, true);
                }
                else if(log.GetLogRecordType()==LogType::INSERT_ENTRY){
                    // std::cout<<"7\n";
//                    std::cout<<"in undo,log type="<<LogTypeStr[log.GetLogRecordType()]<<"\n";
                    log.deserialize_index(log_buffer_.buffer_+buffer_offset_);
                    Rid cur_rid = log.GetRid();
                    auto ix_name=log.get_index_name();
                    std::string index_name{ix_name,log.get_index_name_size()};
                    auto key_len=log.get_key_size();
                    char* key=new char[key_len];
                    memcpy(key,log.get_key(),key_len);
                    auto ih=sm_manager_->ihs_.at(index_name).get();
//                    std::cout<<"insert key: id="<<*(int*)(key)<<", u="<<*(float*)(key+7)<<"\n";
//                    std::cout<<"insert key: id="<<*(int*)(key)<<"\n";

                    ih->insert_entry(key,cur_rid, nullptr);
                    delete[] key;
                }
                else if(log.GetLogRecordType()==LogType::DELETE_ENTRY){
                    // std::cout<<"8\n";
                    log.deserialize_index(log_buffer_.buffer_+buffer_offset_);
                    auto key_len=log.get_key_size();
                    char* key=new char[key_len];
                    memcpy(key,log.get_key(),key_len);
                    auto ix_name=log.get_index_name();
                    std::string index_name{ix_name,log.get_index_name_size()};
                    auto ih=sm_manager_->ihs_.at(index_name).get();
//                    std::cout<<"delete key: id="<<*(int*)(key)<<", u="<<*(float*)(key+7)<<"\n";
//                    std::cout<<"delete key: id="<<*(int*)(key)<<"\n";
                    ih->delete_entry(key, nullptr);
                    delete[] key;
                }


            } // idu全部处理完了
            buffer_offset_ += log.GetSize();
            //设定   2000 为一个安全范围，防止需要拼日志

            if(buffer_offset_ +1000 >LOG_BUFFER_SIZE)//此时offset指向log buffer中上一个log的结尾
            {
                tail_length=LOG_BUFFER_SIZE-buffer_offset_;
                char * remain_string=new char[tail_length];
                memcpy(remain_string,log_buffer_.buffer_+buffer_offset_,tail_length);//log_buffer_.buffer_+buffer_offset_ 开头的tail_length长度为残留字符串
                finished_length=buffer_offset_;//已经扫描过的长度
                memset(log_buffer_.buffer_,0,LOG_BUFFER_SIZE);
                memcpy(log_buffer_.buffer_,remain_string,tail_length);
                log.clean();
                delete [] remain_string;
                break;
            }
            log.clean();
            log.deserialize(log_buffer_.buffer_ + buffer_offset_);
        }
        if(tail_length==0){ //没有残留字符串则读取一整个buffer
            log_buffer_.offset_ += LOG_BUFFER_SIZE;
        }else{
            log_buffer_.offset_ += finished_length;
        }

    }
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    char buffer[PAGE_SIZE];

    for (const auto& pair : active_txn_) {
        if (std::find(active_txn_list_.begin(), active_txn_list_.end(), pair.first) == active_txn_list_.end()) {
            active_txn_list_.push_back(pair.first);
        }
    }
    std::sort(active_txn_list_.begin(), active_txn_list_.end(), std::greater<txn_id_t>());

    // 遍历活动事务
    for(auto it = active_txn_list_.begin(); it != active_txn_list_.end(); ++it)
    {
        int offset_ = lsn_mapping_[active_txn_[*it]];
        LogRecord log;
        // std::cout<<"a\n";
        // std::cout<<"in undo: the current lsn is "<<it->second<<"\n";
        disk_manager_->read_log(buffer, PAGE_SIZE, offset_);
        log.deserialize(buffer);
        while(log.lsn_ != INVALID_LSN && log.log_tid_ != INVALID_TXN_ID)
        {
            // std::cout<<"b\n";
            if(log.GetLogRecordType() == LogType::BEGIN)
            {
                break;
            }
            else if(log.GetLogRecordType() == LogType::INSERT)
            {
                // std::cout<<"c\n";
                // std::cout<<"undo insert\n";
                log.deserialize_i_and_d(buffer);
                Rid cur_rid = log.GetRid();
                // TODO(zjm) use bpm and page to delete
                auto tblname = log.get_table_name();
                std::string file_name(tblname, log.get_table_name_size());
                    auto fh = sm_manager_->fhs_[file_name].get();
                    Context* context = nullptr;
                    fh->delete_record(cur_rid,context);
            }
            else if(log.GetLogRecordType() == LogType::DELETE)
            {
                // std::cout<<"d\n";
                // std::cout<<"undo delete\n";
                log.deserialize_i_and_d(buffer);
                Rid cur_rid = log.GetRid();
                // TODO(zjm) use bpm and page to insert
                auto tblname = log.get_table_name();
                std::string file_name(tblname, log.get_table_name_size());
                    auto fh = sm_manager_->fhs_[file_name].get();
                    char* val = new char[log.GetValue().size+1];
                    memcpy(val,log.GetValue().data,log.GetValue().size);
                    fh->insert_record(cur_rid,val);
                    delete [] val;
            }
            else if(log.GetLogRecordType() == LogType::UPDATE)
            {
                //  std::cout<<"e\n";
                // std::cout<<"undo upd\n";
                log.deserialize_upd(buffer);
                Rid cur_rid = log.GetRid();
                // TODO(zjm) use bpm and page to update
                auto tblname = log.get_table_name();
                std::string file_name(tblname, log.get_table_name_size());
                    auto fh = sm_manager_->fhs_[file_name].get();
                    char* val = new char[log.GetOldValue().size+1];
                    memcpy(val,log.GetOldValue().data,log.GetOldValue().size);
                    Context* context = nullptr;
                    fh->update_record(cur_rid,val,context);
                    delete [] val;
            }
            else if(log.GetLogRecordType()==LogType::INSERT_ENTRY){
                // undo insert entry
//                std::cout<<"in undo,log type="<<LogTypeStr[log.GetLogRecordType()]<<"\n";
                log.deserialize_index(buffer);
                Rid cur_rid = log.GetRid();
                auto key_len=log.get_key_size();
                char* key=new char[key_len];
                memcpy(key,log.get_key(),key_len);
                auto ix_name=log.get_index_name();
                std::string index_name{ix_name,log.get_index_name_size()};
                auto ih=sm_manager_->ihs_.at(index_name).get();
//                std::cout<<"key="<<*(int*)key<<"\n";
                ih->delete_entry(key, nullptr);
                delete[] key;
            }
            else if(log.GetLogRecordType()==LogType::DELETE_ENTRY){
                log.deserialize_index(buffer);
                Rid cur_rid = log.GetRid();
                auto ix_name=log.get_index_name();
                std::string index_name{ix_name,log.get_index_name_size()};
                auto key_len=log.get_key_size();
                char* key=new char[key_len];
                memcpy(key,log.get_key(),key_len);
                auto ih=sm_manager_->ihs_.at(index_name).get();
                ih->insert_entry(key,cur_rid, nullptr);
                delete[] key;
            }
            if(log.prev_lsn_==INVALID_LSN) break;
            offset_ = lsn_mapping_[log.prev_lsn_];
            disk_manager_->read_log(buffer, PAGE_SIZE, offset_);
            log.deserialize(buffer);
        }
    }
    active_txn_.clear();
    lsn_mapping_.clear();
    active_txn_list_.clear();
}
