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

#include <atomic>

#include "common/config.h"
#include "defs.h"
#include "record/rm_defs.h"

/* 标识事务状态 */
enum class TransactionState { DEFAULT, GROWING, SHRINKING, COMMITTED, ABORTED };

/* 系统的隔离级别，当前赛题中为可串行化隔离级别 */
enum class IsolationLevel { READ_UNCOMMITTED, REPEATABLE_READ, READ_COMMITTED, SERIALIZABLE };

/* 事务写操作类型，包括插入、删除、更新三种操作 */
enum class WType { INSERT_TUPLE = 0, DELETE_TUPLE, UPDATE_TUPLE};

/**
 * @brief 事务的写操作记录，用于事务的回滚
 * INSERT
 * --------------------------------
 * | wtype | tab_name | tuple_rid |
 * --------------------------------
 * DELETE / UPDATE
 * ----------------------------------------------
 * | wtype | tab_name | tuple_rid | tuple_value |
 * ----------------------------------------------
 */
class WriteRecord {
   public:
    WriteRecord() = default;

    // constructor for insert operation
    WriteRecord(WType wtype, const std::string &tab_name, const Rid &rid)
        : wtype_(wtype), tab_name_(tab_name), rid_(rid) {}

    // constructor for delete & update operation
    WriteRecord(WType wtype, const std::string &tab_name, const Rid &rid, const RmRecord &record)
        : wtype_(wtype), tab_name_(tab_name), rid_(rid), record_(record) {}

    WriteRecord(WType wtype, const std::string &tab_name, const Rid &rid, const RmRecord &record, const RmRecord &new_rec)
            : wtype_(wtype), tab_name_(tab_name), rid_(rid), record_(record), new_rec_(new_rec) {}
    ~WriteRecord() = default;

    inline RmRecord &GetRecord() {
        /*
        if(record_.allocated_== false) {
            throw std::runtime_error("record is not allocated");
        }
         */
        return record_; }

    inline RmRecord &GetNewRec() {
        return new_rec_;
    }

    inline Rid &GetRid() { return rid_; }

    inline WType &GetWriteType() { return wtype_; }

    inline std::string &GetTableName() { return tab_name_; }
    // 在WriteRecord类中重载相等运算符
    bool operator==(const WriteRecord& other) const {
        return wtype_ == other.wtype_ &&
               tab_name_ == other.tab_name_ &&
               rid_ == other.rid_;
    }


private:
    WType wtype_;
    std::string tab_name_;
    Rid rid_;
    RmRecord record_;
    RmRecord new_rec_;
};
class IndexWriteRecord {
public:
    IndexWriteRecord() = default;

    // constructor for insert operation
    IndexWriteRecord(WType wtype, const std::string &tab_name, const Rid &rid, const size_t ix_num, char* key,int len)
            : wtype_(wtype), tab_name_(tab_name), rid_(rid), ix_num_(ix_num), key_(key), len_(len) {}

    // constructor for delete & update operation
    IndexWriteRecord(WType wtype, const std::string &tab_name,  const size_t ix_num, char* key,int len)
            : wtype_(wtype), tab_name_(tab_name), ix_num_(ix_num), key_(key), len_(len) {}

    ~IndexWriteRecord(){
        if (key_!= nullptr){
            delete [] key_;
            key_ = nullptr;
        }
    }
    inline char* GetKey() {return key_;}
    inline size_t GetIx() {return ix_num_;}
    inline int GetLen() {return len_;}
    inline Rid &GetRid() { return rid_; }

    inline WType &GetWriteType() { return wtype_; }

    inline std::string &GetTableName() { return tab_name_; }
    // 在WriteRecord类中重载相等运算符
    bool operator==(const IndexWriteRecord& other) const {
        return wtype_ == other.wtype_ &&
               tab_name_ == other.tab_name_ &&
               rid_ == other.rid_;
    }
private:
    WType wtype_;
    std::string tab_name_;
    Rid rid_;
    RmRecord record_;
    size_t ix_num_;
    char* key_{nullptr};
    int len_;
};

/* 多粒度锁，加锁对象的类型，包括记录和表 */
enum class LockDataType { TABLE = 0, RECORD = 1 };

/**
 * @description: 加锁对象的唯一标识
 */
class LockDataId {
   public:
    /* 表级锁 */
    LockDataId(int fd, LockDataType type) {
        assert(type == LockDataType::TABLE);
        fd_ = fd;
        type_ = type;
        rid_.page_no = -1;
        rid_.slot_no = -1;
    }

    /* 行级锁 */
    LockDataId(int fd, const Rid &rid, LockDataType type) {
        assert(type == LockDataType::RECORD);
        fd_ = fd;
        rid_ = rid;
        type_ = type;
    }

    inline int64_t Get() const {
        if (type_ == LockDataType::TABLE) {
            // fd_
            return static_cast<int64_t>(fd_);
        } else {
            // fd_, rid_.page_no, rid.slot_no
            return ((static_cast<int64_t>(type_)) << 63) | ((static_cast<int64_t>(fd_)) << 31) |
                   ((static_cast<int64_t>(rid_.page_no)) << 16) | rid_.slot_no;
        }
    }

    bool operator==(const LockDataId &other) const {
        if (type_ != other.type_) return false;
        if (fd_ != other.fd_) return false;
        return rid_ == other.rid_;
    }
    int fd_;
    Rid rid_;
    LockDataType type_;
};

template <>
struct std::hash<LockDataId> {
    size_t operator()(const LockDataId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

/* 事务回滚原因 */
enum class AbortReason { LOCK_ON_SHIRINKING = 0, UPGRADE_CONFLICT, DEADLOCK_PREVENTION, FAILED_TO_LOCK, ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD};

/* 事务回滚异常，在rmdb.cpp中进行处理 */
class TransactionAbortException : public std::exception {
    txn_id_t txn_id_;
    AbortReason abort_reason_;

   public:
    explicit TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason)
        : txn_id_(txn_id), abort_reason_(abort_reason) {}

    txn_id_t get_transaction_id() { return txn_id_; }
    AbortReason GetAbortReason() { return abort_reason_; }
    std::string GetInfo() {
        switch (abort_reason_) {
            case AbortReason::LOCK_ON_SHIRINKING: {
                return "Transaction " + std::to_string(txn_id_) +
                       " aborted because it cannot request locks on SHRINKING phase\n";
            } break;

            case AbortReason::UPGRADE_CONFLICT: {
                return "Transaction " + std::to_string(txn_id_) +
                       " aborted because another transaction is waiting for upgrading\n";
            } break;

            case AbortReason::DEADLOCK_PREVENTION: {
                return "Transaction " + std::to_string(txn_id_) + " aborted for deadlock prevention\n";
            } break;

            case AbortReason::FAILED_TO_LOCK: {
                return "Transaction " + std::to_string(txn_id_) + " aborted for failing to lock\n";
            } break;

            case AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD:
                return "Transaction " + std::to_string(txn_id_) + " aborted because attempted to unlock but no lock held \n";
                break;

            default: {
                return "Transaction aborted\n";
            } break;
        }
    }
};