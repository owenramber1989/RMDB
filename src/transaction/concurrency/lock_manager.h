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
#include <condition_variable>
#include "transaction/transaction.h"
#include "common/config.h"

class TransactionManager;
static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

class LockManager {
    /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁） */

    /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
    enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX};

    /* 事务的加锁申请 */
    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode, LockDataId lockDataId)
            : txn_id_(txn_id), lock_mode_(lock_mode), lockDataId_(lockDataId), granted_(false) {}
        txn_id_t txn_id_;   // 申请加锁的事务ID
        LockMode lock_mode_;    // 事务申请加锁的类型
        LockDataId lockDataId_;
        bool granted_;          // 该事务是否已经被赋予锁
    };

    /* 数据项上的加锁队列 */
    class LockRequestQueue {
    public:
        std::list<LockRequest*> request_queue_;  // 加锁队列
        std::condition_variable cv_;            // 条件变量，用于唤醒正在等待加锁的申请，在no-wait策略下无需使用
        size_t x_cnt_ = 0;
        /** txn_id of an upgrading transaction (if any) */
        txn_id_t upgrading_ = INVALID_TXN_ID;
        std::mutex latch_;

        std::unordered_map<txn_id_t, LockRequest *> locked_requests_; // 持锁队列
        std::condition_variable all_recv_cv_;
    };

public:
    explicit LockManager(TransactionManager* txn_manager) {
        txn_manager_ = txn_manager;
    }

    ~LockManager() = default;

    void StartDeadlockDetection() {
        enable_cycle_detection_ = true;
        cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
    }

    static auto CanTxnTakeLock(std::shared_ptr<Transaction> txn) -> bool;

    bool lock_on_table(std::shared_ptr<Transaction> txn, int tab_fd, LockMode lockMode);

    bool unlock(std::shared_ptr<Transaction> txn, LockDataId lock_data_id);

    struct WaitPairHashFunction {
        auto operator()(const std::pair<txn_id_t, txn_id_t> &x) const -> size_t { return x.first ^ x.second; }
    };

    /*** Graph API ***/

    /**
     * Adds an edge from t1 -> t2 from waits for graph.
     * @param t1 transaction waiting for a lock
     * @param t2 transaction being waited for
     */
    auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

    /**
     * Removes an edge from t1 -> t2 from waits for graph.
     * @param t1 transaction waiting for a lock
     * @param t2 transaction being waited for
     */
    auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

    /**
     * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
     * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
     * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
     */
    auto HasCycle(txn_id_t *txn_id) -> bool;


    /**
     * Runs cycle detection in the background.
     */
    auto RunCycleDetection() -> void;

    auto FindCycle(txn_id_t source_txn, std::unordered_set<txn_id_t> &on_path,
                   std::unordered_map<txn_id_t, std::shared_ptr<Transaction>> &txns,
                   std::unordered_set<std::pair<txn_id_t, txn_id_t>, WaitPairHashFunction> &pairs) -> bool;

    auto UpgradeLockTable(std::shared_ptr<Transaction> txn, LockMode lock_mode, LockMode new_lock_mode,
                          const LockRequestQueue *qu, const LockDataId &oid) -> bool;


    auto IsCompatible(LockMode l1, std::shared_ptr<LockRequestQueue> &req_queue, LockRequest *cur_req) -> bool;

private:
    std::mutex latch_;      // 用于锁表的并发
    std::unordered_map<LockDataId, std::shared_ptr<LockRequestQueue>> lock_table_;   // 全局锁表

    std::unordered_map<txn_id_t, std::shared_ptr<LockRequestQueue>> wait_for_lock_map_;
    // 维护持锁事务和对应请求队列的map
    std::mutex wait_for_map_latch_;
    std::atomic<bool> enable_cycle_detection_;
    std::thread *cycle_detection_thread_;
    std::unordered_set<std::pair<txn_id_t, txn_id_t>, WaitPairHashFunction> wait_pairs_;
    std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
    // 维护wait-for图
    std::unordered_map<txn_id_t, std::shared_ptr<Transaction>> txns_;
    std::mutex waits_for_latch_;
    TransactionManager* txn_manager_;
};
