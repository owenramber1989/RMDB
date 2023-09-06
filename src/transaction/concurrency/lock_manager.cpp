/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"
#include "transaction/transaction_manager.h"


std::chrono::milliseconds cycle_detection_interval(50);

auto LockManager::CanTxnTakeLock(std::shared_ptr<Transaction> txn) -> bool {
    txn->LockTxn();
    auto txn_state = txn->get_state();
    if (txn_state == TransactionState::ABORTED) {
        txn->UnlockTxn();
        return false;
    }
    txn->UnlockTxn();
    return true;
}

bool LockManager::lock_on_table(std::shared_ptr<Transaction> txn, int tab_fd, LockMode lock_mode) {
    if (txn == nullptr) {
        return false;
    }
    if (!CanTxnTakeLock(txn)) {
        return false;
    }
    std::shared_ptr<LockRequestQueue> request_queue;
    auto txn_id = txn->get_transaction_id();
    std::unique_lock<std::mutex> table_lock(latch_);
    auto tab_id = LockDataId(tab_fd, LockDataType::TABLE);
    if (lock_table_.count(tab_id) == 0) {
        lock_table_[tab_id] = std::make_shared<LockRequestQueue>();
    }
    request_queue = lock_table_[tab_id];
    std::unique_lock<std::mutex> lock(request_queue->latch_);
    table_lock.unlock();

    LockRequest *request{nullptr};
    if (request_queue->locked_requests_.count(txn_id) != 0) {
        request = request_queue->locked_requests_[txn_id];
        if (request->lock_mode_ == lock_mode || request->lock_mode_ == LockMode::EXCLUSIVE) {
            return true;
        }
        UpgradeLockTable(txn, request->lock_mode_, lock_mode, request_queue.get(), tab_id); // 这里其实只判断了之前有没有事务发起了升级请求（并且尚未满足）
        request->lock_mode_ = lock_mode;
        request_queue->upgrading_ = txn_id;
        request_queue->locked_requests_.erase(txn_id); // 现在持有的s锁可以先放掉，加强并发
    }
    LockRequest *new_request{nullptr};
    if (request_queue->upgrading_ == txn_id) {
        new_request = request;
    } else { // 当前事务并未持锁
        new_request = new LockRequest(txn_id, lock_mode, tab_id);
    }

    // std::unique_lock<std::mutex> lock(request_queue->latch_);
    // request_queue->request_queue_.push_back(new_request);
    if (request_queue->locked_requests_.empty()) { // 当前数据项尚无其他事务持锁
        if (request_queue->upgrading_ == txn_id || request_queue->request_queue_.empty()) {
            if (request_queue->upgrading_ == txn_id) {
                request_queue->upgrading_ = INVALID_TXN_ID;
            }
            request_queue->locked_requests_.insert({txn_id, new_request});
            txn->get_lock_set()->insert(tab_id);
            return true;
        }
    }
    // 当前尚有其他事务持锁
    {
        std::unique_lock<std::mutex> wait_lock(wait_for_map_latch_);
        wait_for_lock_map_[txn_id] = request_queue;
    }

    if (request_queue->upgrading_ == txn_id) {
        request_queue->request_queue_.push_front(new_request);
    } else {
        request_queue->request_queue_.push_back(new_request);
    }

    TransactionState state;
    request_queue->cv_.wait(lock, [&]() -> bool { // 等待当前数据项请求队列上的锁
        txn->LockTxn();
        state = txn->get_state();
        if (state == TransactionState::ABORTED) {
            txn->UnlockTxn();
            return true;
        }
        txn->UnlockTxn();
        // 其实这个函数就是看请求队列和持锁队列是否都是s锁，因为目前的实现只有s和x两种锁
        if (!IsCompatible(request_queue->request_queue_.front()->lock_mode_, request_queue,
                          request_queue->request_queue_.front())) {
            return false;
        }
        return IsCompatible(lock_mode, request_queue, new_request);
        // 兼容性已满足，可以不用再等待请求队列上面的锁了
    });
    // 如果第一个请求就是当前事务申请的x锁，上面的逻辑会不会出问题?
    {
        std::unique_lock<std::mutex> wait_lock(wait_for_map_latch_);
        wait_for_lock_map_.erase(txn_id);
    } // 当前事务已经不用再等待锁了

    if (state == TransactionState::ABORTED) { // 因为别的事务死锁检测可能把它给abort了
        auto iter = std::find(request_queue->request_queue_.begin(), request_queue->request_queue_.end(), new_request);
        if (iter != request_queue->request_queue_.end()) {
            request_queue->request_queue_.erase(iter);
        }

        if (request_queue->upgrading_ == txn_id) {
            request_queue->upgrading_ = INVALID_TXN_ID;
        }

        delete new_request;
        request_queue->cv_.notify_all();
        return false;
    }

    if (request_queue->upgrading_ == txn_id) {
        request_queue->upgrading_ = INVALID_TXN_ID;
        request_queue->request_queue_.pop_front();
    } else {
        auto iter = std::find(request_queue->request_queue_.begin(), request_queue->request_queue_.end(), new_request);
        if (iter != request_queue->request_queue_.end()) {
            request_queue->request_queue_.erase(iter);
        }
    }
    request_queue->locked_requests_.insert({txn_id, new_request});
    txn->get_lock_set()->insert(tab_id);
    return true;
}
 // 因为只加了表锁，所以下面默认为释放表锁，以后如果升级成行锁一定要改
bool LockManager::unlock(std::shared_ptr<Transaction> txn, LockDataId lock_data_id) {
    if(txn == nullptr) {
        return false;
    }
    if(txn->get_lock_set()->count(lock_data_id)==0) {
        txn->LockTxn();
        txn->set_state(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
    auto txn_id = txn->get_transaction_id();
    std::shared_ptr<LockRequestQueue> request_queue;
    std::unique_lock<std::mutex> lock_table_lock(latch_);
    request_queue = lock_table_[lock_data_id];
    std::unique_lock<std::mutex> request_queue_lock(request_queue->latch_);
    lock_table_lock.unlock();
    auto request = request_queue->locked_requests_[txn_id];
    // txn->get_lock_set()->erase(lock_data_id);
    request_queue->locked_requests_.erase(txn_id);
    delete request;
    request_queue->cv_.notify_all();
    request_queue_lock.unlock();
    return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
    if (std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2) != waits_for_[t1].end()) {
        return;
    }
    waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    std::vector<txn_id_t>::iterator iter;
    if (waits_for_.count(t1) == 0 ||
        (iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2)) == waits_for_[t1].end()) {
        return;
    }

    waits_for_[t1].erase(iter);
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
    std::vector<txn_id_t> source_txns;
    for (auto &from_txn_info : waits_for_) {
        source_txns.push_back(from_txn_info.first);
    }
    std::sort(source_txns.begin(), source_txns.end());

    for (const auto src_txn_id : source_txns) {
        std::unordered_set<txn_id_t> on_path;
        on_path.insert(src_txn_id);
        std::unordered_set<std::pair<txn_id_t, txn_id_t>, WaitPairHashFunction> pairs;
        auto res = FindCycle(src_txn_id, on_path, txns_, pairs);
        if (res) { // on_path中有环存在，牺牲最年轻的事务
            txn_id_t max_id = INVALID_TXN_ID;
            for (auto id : on_path) {
                max_id = std::max(id, max_id);
            }
            // 目前是直接牺牲最年轻的事务，实际上还可以更精确一点，比如持有最少锁的事务（加锁很昂贵）or 处理元组最少的事务
            *txn_id = max_id;
            return res;
        }
    }
    return false;
}

auto LockManager::FindCycle(txn_id_t source_txn, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_map<txn_id_t, std::shared_ptr<Transaction>> &txns,
                            std::unordered_set<std::pair<txn_id_t, txn_id_t>, WaitPairHashFunction> &pairs) -> bool {
    if (waits_for_.count(source_txn) == 0) {
        return false;
    }
    auto &to_txns = waits_for_[source_txn];
    int size = to_txns.size();
    for (int i = 0; i < size; ++i) {
        auto txn_id = to_txns[i];
        if (on_path.count(txn_id) != 0) {
            return true;
        } // 因为目前是单向访问事务id，如果发生了重复访问，只能说明有环存在
        on_path.insert(txn_id);
        auto res = FindCycle(txn_id, on_path, txns, pairs);
        if (res) {
            return res;
        }
        on_path.erase(txn_id);
    }
    return false;
}

void LockManager::RunCycleDetection() {
    while (enable_cycle_detection_) {
        std::this_thread::sleep_for(cycle_detection_interval);
        {
            std::unique_lock<std::mutex> wait_for_lock(waits_for_latch_);
            waits_for_.clear();

            std::scoped_lock<std::mutex> lock(latch_);

            for (auto &[oid, req_queue] : lock_table_) {
                std::unique_lock<std::mutex> queue_lock(req_queue->latch_);
                for (auto [txn_id, locked_req] : req_queue->locked_requests_) {
                    auto locked_txn_id = locked_req->txn_id_;
                    for (auto &req : req_queue->request_queue_) {
                        AddEdge(req->txn_id_, locked_txn_id);
                    }
                }
            }

            for (auto &[txn_id, txn_vec] : waits_for_) {
                sort(txn_vec.begin(), txn_vec.end());
            } // 从小到大，维护确定性


            txn_id_t need_abort_txn_id = INVALID_TXN_ID;
            while (HasCycle(&need_abort_txn_id)) {
                std::shared_ptr<Transaction> need_abort_txn = txn_manager_->get_transaction(need_abort_txn_id);
                need_abort_txn->UnlockTxn();
                need_abort_txn->set_state(TransactionState::ABORTED);
                need_abort_txn->UnlockTxn();

                std::shared_ptr<LockRequestQueue> request_queue;
                std::unique_lock<std::mutex> wait_lock(wait_for_map_latch_);
                if (wait_for_lock_map_.count(need_abort_txn_id) != 0) {
                    request_queue = wait_for_lock_map_[need_abort_txn_id];
                } else {
                    if (waits_for_.count(need_abort_txn_id) != 0) {
                        waits_for_.erase(need_abort_txn_id);
                    }

                    for (auto &waits : waits_for_) {
                        auto iter = std::find(waits.second.begin(), waits.second.end(), need_abort_txn_id);
                        if (iter != waits.second.end()) {
                            waits.second.erase(iter);
                        }
                    }

                    need_abort_txn_id = INVALID_TXN_ID;
                    continue; // 没有其他事务在等待当前事务，处理当前事务的下一个环（如果有的话）
                } // 还有其他事务在等待当前事务，让这些事务获得锁！
                std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
                wait_lock.unlock();
                request_queue->cv_.notify_all();
                queue_lock.unlock();

                if (waits_for_.count(need_abort_txn_id) != 0) {
                    waits_for_.erase(need_abort_txn_id);
                }

                for (auto &waits : waits_for_) {
                    auto iter = std::find(waits.second.begin(), waits.second.end(), need_abort_txn_id);
                    if (iter != waits.second.end()) {
                        waits.second.erase(iter);
                    }
                }
                need_abort_txn_id = INVALID_TXN_ID;
            }
        }
    }
}

auto LockManager::UpgradeLockTable(std::shared_ptr<Transaction> txn, LockMode lock_mode,
                                   LockMode new_lock_mode, const LockRequestQueue *qu,
                                   const LockDataId &tab_id) -> bool {
    if (qu->upgrading_ != INVALID_TXN_ID) {
        txn->LockTxn();
        txn->set_state(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
    }
    txn->get_lock_set()->erase(tab_id);
    return true;
}

auto LockManager::IsCompatible(LockMode l1, std::shared_ptr<LockRequestQueue> &req_queue,
                               LockRequest *cur_req) -> bool {
    // 其实就是判断当前持锁队列和请求队列是否都是s锁，是否都兼容
    for (auto &[txn_id, req] : req_queue->locked_requests_) {
        LockMode l2 = req->lock_mode_;
        if (!(l1==LockMode::SHARED&&l2==LockMode::SHARED)) {
            return false;
        }
    }

    for (auto &iter : req_queue->request_queue_) {
        if (iter == cur_req) {
            break;
        } // 如果当前请求已经在请求队列里面了，那显然都是兼容的

        LockMode l2 = iter->lock_mode_;
        if (!(l1==LockMode::SHARED&&l2==LockMode::SHARED)) {
            return false;
        }
    }
    return true;
}
