/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include <chrono>
#include <thread>

#include "log_manager.h"

std::atomic<bool> enable_logging(false);


/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
/*
lsn_t LogManager::add_log_to_buffer(LogRecord *log_record) {
    auto size = log_record->GetSize();
    std::unique_lock<std::mutex> guard(log_latch_);
    std::unique_lock<std::mutex> guard2(latch_);
    log_record->lsn_ = global_lsn_++;
    // std::cout<<"in add_log_to_buffer: the current lsn is "<<log_record->lsn_<<"\n";
    if (size + log_buffer_size_ > LOG_BUFFER_SIZE) {
        // std::cout<<"0\n";
        // 叫醒后台线程
        GetBgTaskToWork();
        guard2.unlock();
        WaitUntilBgTaskFinish();
        assert(log_buffer_size_ == 0);
        guard2.lock();
    }
    char* buf = new char[log_record->GetSize()];
    log_record->serialize(buf);
    if(log_record->GetLogRecordType()==LogType::UPDATE){
        // std::cout<<"in add_log_to_buffer: the type is "<<LogTypeStr[log_record->GetLogRecordType()]<<"\n";
        log_record->serialize_upd(buf);
    } else if(log_record->GetLogRecordType()==LogType::INSERT || log_record->GetLogRecordType()==LogType::DELETE) {
        // std::cout<<"in add_log_to_buffer: the type is "<<LogTypeStr[log_record->GetLogRecordType()]<<"\n";
        log_record->serialize_i_and_d(buf);
    }else if(log_record->GetLogRecordType()==LogType::INSERT_ENTRY||log_record->GetLogRecordType()==LogType::DELETE_ENTRY){
        log_record->serialize_index(buf);
    }
    else if (log_record->GetLogRecordType()!=LogType::INVALID){
        // std::cout<<"in add_log_to_buffer: the type is "<<LogTypeStr[log_record->GetLogRecordType()]<<"\n";
        // log_record->serialize(buf);
    }
    memcpy(log_buffer_+log_buffer_size_, buf, size);
    delete [] buf;
    log_buffer_size_ += size;
    cv_.notify_all();
    return log_record->GetLSN();
}
*/

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
    auto size = log_record->GetSize();
    std::unique_lock<std::mutex> guard(latch_);
    log_record->lsn_ = global_lsn_++;
    char* buf = new char[log_record->GetSize()];
    log_record->serialize(buf);
    if(log_record->GetLogRecordType()==LogType::UPDATE){
        log_record->serialize_upd(buf);
    } else if(log_record->GetLogRecordType()==LogType::INSERT || log_record->GetLogRecordType()==LogType::DELETE) {
        log_record->serialize_i_and_d(buf);
    }else if(log_record->GetLogRecordType()==LogType::INSERT_ENTRY||log_record->GetLogRecordType()==LogType::DELETE_ENTRY){
        log_record->serialize_index(buf);
    }
    disk_manager_->write_log(buf, size);
    delete [] buf;
    return log_record->GetLSN();
}


/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
    while (flush_thread_on) {
        // 下面这对大括号是为了限定锁的作用域
        {
            // std::cout<<"flush_thread is on\n";
            std::unique_lock<std::mutex> lock(latch_);
            while (log_buffer_size_ == 0) {
                // std::cout<<"the log buffer size is 0\n";
                // 对于下面应该用log_timeout还是FLUSH_TIMEOUT我感到困惑
                auto ret = cv_.wait_for(lock, FLUSH_TIMEOUT);
                if (ret == std::cv_status::no_timeout || !flush_thread_on) {
                    break;
                }
            }
            SwapBuffer();
        }
        // std::cout<<"we're gonna write to the disk\n";
        // 写入磁盘
        // std::cout<<"before write_log\n";
        // std::cout<<"the size of flush buffer is: "<<flush_buffer_size_<<std::endl;
        disk_manager_->write_log(flush_buffer_, flush_buffer_size_);
        // std::cout<<"after write_log\n";
        std::unique_lock<std::mutex> lock(latch_);
        // std::cout<<"after lock\n";
        auto lsn = lastLsn(flush_buffer_, flush_buffer_size_);
        // std::cout<<"in flush_log_to_disk: the last lsn is "<<lsn<<"\n";
        if (lsn != INVALID_LSN) {
            // std::cout<<"the new persistent lsn is "<<lsn<<"\n";
            SetPersistentLSN(lsn);
        }
        flush_buffer_size_ = 0;
        flushed.notify_all();
    }
}

int LogManager::lastLsn(char* buff, int size) {
    lsn_t cur = INVALID_LSN;
    char *ptr = buff;
    // std::cout<<"下面是相差的部分\n";
    // std::cout<<buff+size-ptr<<std::endl;
    // insert 的时候会在下面死循环
    while (ptr < buff + size) {
        // std::cout<<"进入了一次while\n";
        LogRecord rec;
        rec.deserialize(ptr);
        if(rec.GetLogRecordType()==LogType::BEGIN||rec.GetLogRecordType()==LogType::COMMIT||rec.GetLogRecordType()==LogType::ABORT) {
        } else if(rec.GetLogRecordType()==LogType::UPDATE) {
            rec.deserialize_upd(ptr);
        } else if(rec.GetLogRecordType()==LogType::INSERT||rec.GetLogRecordType()==LogType::DELETE){
            rec.deserialize_i_and_d(ptr);
        } else if(rec.GetLogRecordType()==LogType::INSERT_ENTRY || rec.GetLogRecordType()==LogType::DELETE_ENTRY) {
            rec.deserialize_index(ptr);
        }
        else {
            break;
        }
        cur = rec.GetLSN();
        auto len = rec.GetSize();
        ptr = ptr + len;
    }
    return cur;
}

/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
    std::unique_lock<std::mutex> lock(latch_);
    if (flush_thread_on) {
        flush_thread_on = false;
        enable_logging = false;

        lock.unlock();
        cv_.notify_all();

        flush_thread_->join();
        lock.lock();
        delete flush_thread_;
    }
}

// 阻塞地完成flush
void LogManager::FlushNowBlocking() {
    GetBgTaskToWork();
    WaitUntilBgTaskFinish();
}

// 交换log_buffer_和flush_buffer_
void LogManager::SwapBuffer() {
    std::swap(flush_buffer_, log_buffer_);
    flush_buffer_size_ = log_buffer_size_;
    log_buffer_size_ = 0;
}

// 启动后台线程
void LogManager::GetBgTaskToWork() {
    cv_.notify_all();
}

// 等待flush任务的结束
void LogManager::WaitUntilBgTaskFinish() {
    std::unique_lock<std::mutex> condWait(latch_);
    while (flush_buffer_size_ != 0) {
        flushed.wait(condWait);
    }
}

/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {
    std::lock_guard<std::mutex> guard(latch_);
    if (!flush_thread_on) {
        enable_logging = true;
        // true表明flush thread 在运行
        flush_thread_on = true;
        flush_thread_ = new std::thread(&LogManager::flush_log_to_disk, this);
    }
}