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

#include <mutex>
#include <vector>
#include <future>
#include <condition_variable>
#include <iostream>
#include "log_defs.h"
#include "common/config.h"
#include "record/rm_defs.h"

/* 日志记录对应操作的类型 */
enum LogType: int {
    INVALID = 0,
    UPDATE,
    INSERT,
    DELETE,
    BEGIN,
    COMMIT,
    ABORT,
    INSERT_ENTRY,
    DELETE_ENTRY
};
static std::string LogTypeStr[] = {
        "INVALID",
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
        "ABORT",
        "INSERT_ENTRY",
        "DELETE_ENTRY"

};

class LogRecord {

public:
    LogType log_type_;         /* 日志对应操作的类型 */
    lsn_t lsn_;                /* 当前日志的lsn */
    uint32_t log_tot_len_;     /* 整个日志记录的长度 */
    txn_id_t log_tid_;         /* 创建当前日志的事务ID */
    lsn_t prev_lsn_;           /* 事务创建的前一条日志记录的lsn，用于undo */
    Rid rid_;
    // case1: for insert and delete operation
    RmRecord value_;
    // case2: for update operation
    RmRecord old_value_;
    RmRecord new_value_;
    char* table_name_= nullptr;
    size_t table_name_size_;
    // case3: for index
    char* key_= nullptr;
    size_t key_size_;


    inline uint32_t GetSize() const { return log_tot_len_; }

    inline lsn_t GetLSN() const { return lsn_; }

    inline txn_id_t GetTxnId() const { return log_tid_; }

    inline lsn_t GetPrevLSN() const { return prev_lsn_; }

    inline LogType &GetLogRecordType() { return log_type_; }

    inline Rid& GetRid() { return rid_; }

    inline RmRecord& GetValue() { return value_; }

    inline RmRecord& GetOldValue() { return old_value_; }

    inline RmRecord& GetNewValue() { return new_value_; }

    inline char* get_table_name() const { return table_name_; }

    inline size_t get_table_name_size() const { return table_name_size_; }

    inline void set_lsn(lsn_t lsn) { lsn_ = lsn; }
    inline char* get_key()const{return key_;}

    inline size_t get_key_size(){return key_size_;}

    inline char* get_index_name()const{return table_name_;}
    inline size_t get_index_name_size(){return table_name_size_;}

    // 把日志记录序列化到dest中
    virtual void serialize (char* dest) const {
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
    }
    // 从src中反序列化出一条日志记录
    virtual void deserialize(const char* src) {
        log_type_ = *reinterpret_cast<const LogType*>(src);
        lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_LSN);
        log_tot_len_ = *reinterpret_cast<const uint32_t*>(src + OFFSET_LOG_TOT_LEN);
        log_tid_ = *reinterpret_cast<const txn_id_t*>(src + OFFSET_LOG_TID);
        prev_lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_PREV_LSN);
    }
    // used for debug
    virtual void format_print() {
        std::cout << "log type in father_function: " << LogTypeStr[log_type_] << "\n";
        printf("Print Log Record:\n");
        printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
        printf("lsn: %d\n", lsn_);
        printf("log_tot_len: %d\n", log_tot_len_);
        printf("log_tid: %d\n", log_tid_);
        printf("prev_lsn: %d\n", prev_lsn_);
    }
    void clean() {
        log_type_ = LogType::INVALID;
        lsn_ = INVALID_LSN;
        log_tot_len_ = 0;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    LogRecord() = default;
    //begin commit abort 语句通用
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogType log_record_type) {
        log_tot_len_ = LOG_HEADER_SIZE;
        lsn_ = INVALID_LSN;
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;
        log_type_ = log_record_type;
    }
    //insert and delete
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogType log_record_type, const Rid& rid, const RmRecord& value, const std::string& table_name ) {
        log_tot_len_ = LOG_HEADER_SIZE + sizeof(int32_t) + sizeof(Rid) + value.size + table_name.size() + sizeof(size_t);
        lsn_ = INVALID_LSN;
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;
        log_type_ = log_record_type;
        rid_ = rid;
        value_  = value;
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
    }
    //update
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogType log_record_type, const Rid& rid, const RmRecord& old_value, const RmRecord& new_value, const std::string& table_name ) {
        log_tot_len_ = LOG_HEADER_SIZE + sizeof(int32_t)*2 + sizeof(Rid) + old_value.size + new_value.size + table_name.size() +  sizeof(size_t);
        lsn_ = INVALID_LSN;
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;
        log_type_ = log_record_type;
        rid_ = rid;
        old_value_  = old_value;
        new_value_ = new_value;
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
    }
    void serialize_i_and_d(char* dest) const {
        // LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, value_.data, value_.size);
        offset += value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
    }
    // 从src中反序列化出一条Insert日志记录
    void deserialize_i_and_d(const char* src) {
        // LogRecord::deserialize(src);
        value_.Deserialize(src + OFFSET_LOG_DATA);
        int offset = OFFSET_LOG_DATA + value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
    }
    void format_print_i_and_d()  {
        printf("insert/delete record\n");
        LogRecord::format_print();
        printf("insert/delete_value: %s\n", value_.data);
        printf("insert/delete rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_);
    }
    void serialize_upd(char* dest) const  {
        // LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &old_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, old_value_.data, old_value_.size);
        offset += old_value_.size;
        memcpy(dest + offset, &new_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, new_value_.data, new_value_.size);
        offset += new_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
    }
    // 从src中反序列化出一条Insert日志记录
    void deserialize_upd(const char* src)  {
        // LogRecord::deserialize(src);
        old_value_.Deserialize(src + OFFSET_LOG_DATA);
        int offset = OFFSET_LOG_DATA + old_value_.size + sizeof(int);
        new_value_.Deserialize(src + offset);
        offset += new_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
    }
    void format_print_upd()  {
        printf("update record\n");
        LogRecord::format_print();
        printf("old_value: %s\n", old_value_.data);
        printf("new_value: %s\n", new_value_.data);
        printf("update rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_);
    }
    // index
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogType log_record_type, const Rid& rid, char* key,int key_size, const std::string& index_name ) {
        log_tot_len_ = LOG_HEADER_SIZE + sizeof(Rid) + 2*sizeof(size_t) + key_size + index_name.size();
        lsn_ = INVALID_LSN;
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;
        log_type_ = log_record_type;
        rid_ = rid;
        table_name_size_ = index_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, index_name.c_str(), table_name_size_);
        key_size_=key_size;
        key_=new char[key_size_];
        memcpy(key_,key,key_size_);
    }
    // index
    void serialize_index(char* dest) const{
        int offset = OFFSET_LOG_DATA;
        memcpy(dest+offset,&rid_, sizeof(Rid));
        offset+= sizeof(Rid);
        memcpy(dest+offset,&table_name_size_, sizeof(size_t));
        offset+= sizeof(size_t);
        memcpy(dest+offset,table_name_, table_name_size_);
        offset+=table_name_size_;
        memcpy(dest+offset,&key_size_, sizeof(size_t));
        offset+= sizeof(size_t);
        memcpy(dest+offset,key_,key_size_);
    }
    void deserialize_index(const char* src){
        int offset = OFFSET_LOG_DATA;
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
        offset+=table_name_size_;
        key_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        key_ = new char[key_size_];
        memcpy(key_, src + offset, key_size_);
    }
    ~LogRecord(){
        delete [] table_name_;
        delete [] key_;
    }


};

/* 日志缓冲区，只有一个buffer，因此需要阻塞地去把日志写入缓冲区中 */

class LogBuffer {
public:
    LogBuffer() { 
        offset_ = 0; 
        memset(buffer_, 0, sizeof(buffer_));
    }

    bool is_full(int append_size) {
        if(offset_ + append_size > LOG_BUFFER_SIZE)
            return true;
        return false;
    }

    LogBuffer(char* buffer, int size) {
        offset_ = size;
        memset(buffer_, 0, sizeof(buffer_));
        memcpy(buffer_, buffer, size);
    }

    char buffer_[LOG_BUFFER_SIZE+1];
    int offset_;    // 写入log的offset
};

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
class LogManager {
public:
    LogManager(DiskManager* disk_manager) { disk_manager_ = disk_manager;
        log_buffer_ = new char[LOG_BUFFER_SIZE];
        flush_buffer_ = new char[LOG_BUFFER_SIZE];
        flush_thread_on = false;}
    ~LogManager() {
        // StopFlushThread();
        delete[] log_buffer_;
        delete[] flush_buffer_;
        log_buffer_ = nullptr;
        flush_buffer_ = nullptr;
    }
    lsn_t add_log_to_buffer(LogRecord* log_record);
    void flush_log_to_disk();
    char* get_log_buffer() { return log_buffer_; }

    void RunFlushThread();
    void StopFlushThread();
    void FlushNowBlocking();
    void SwapBuffer();
    void GetBgTaskToWork();
    void WaitUntilBgTaskFinish();
    int lastLsn(char* buf, int size);
    lsn_t GetNextLsn() { return global_lsn_; }

    [[nodiscard]] inline lsn_t GetPersistentLSN() const { return persistent_lsn_; }
    inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }

private:    
    std::atomic<lsn_t> global_lsn_{0};  // 全局lsn，递增，用于为每条记录分发lsn
    std::mutex latch_;                  // 用于对log_buffer_的互斥访问
    std::mutex log_latch_;
    char* log_buffer_;              // 日志缓冲区
    char* flush_buffer_;
    lsn_t persistent_lsn_{INVALID_LSN};                 // 记录已经持久化到磁盘中的最后一条日志的日志号
    DiskManager* disk_manager_;

    int log_buffer_size_{0};
    int flush_buffer_size_{0};

    std::thread* flush_thread_;
    std::condition_variable cv_;
    std::condition_variable flushed;
    std::atomic<bool> flush_thread_on;
}; 
