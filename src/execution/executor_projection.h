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


class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        // 注意下面的prev_确实是调用的seq scan的cols
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;

    }

    size_t tupleLen() const override { return len_;}

    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_; }

    [[maybe_unused]] void feed(const std::map<TabCol, Value> &feed_dict)  {
        throw InternalError("Cannot feed a projection node");
    }

    void beginTuple() override {
        // std::cout<<"proj beginTuple"<<std::endl;
        prev_->beginTuple();
    }

    void nextTuple() override {
        // std::cout<<"proj nextTuple"<<std::endl;
        if(prev_->is_end()){
            // std::cout<<"proj's prev_ is ended"<<std::endl;
            return;
        }
        prev_->nextTuple();
    }

    bool has_aggre() override {
        return prev_->has_aggre();
    }

    std::vector<std::unique_ptr<RmRecord>> get_block() override {
        auto blocks = prev_->get_block();
        if((has_aggre()&&get_aggre_type() == ast::AggregationType::COUNT)||(!has_nlj()&&(prev_->cols()==cols_))) {
            return blocks;
        }
        std::vector<std::unique_ptr<RmRecord>> blk;
        for(auto& rec : blocks ) {
            auto &prev_cols = prev_->cols();
            auto prev_rec = std::move(rec);
            auto &proj_cols = cols_;
            auto proj_rec = std::make_unique<RmRecord>(len_);
            for (size_t proj_idx = 0; proj_idx < proj_cols.size(); proj_idx++) {
                size_t prev_idx = sel_idxs_[proj_idx];
                auto &prev_col = prev_cols[prev_idx];
                auto &proj_col = proj_cols[proj_idx];
                // std::cout<<"proj executor len"<<proj_col.len<<std::endl;
                memcpy(proj_rec->data + proj_col.offset, prev_rec->data + prev_col.offset,proj_col.len);
            }
            blk.push_back(std::move(proj_rec));
        }
        return blk;
    }

    std::string get_nickname() override {
        return prev_->get_nickname();
    }
    [[nodiscard]] bool is_end() const override { return prev_->is_end(); }

    std::unique_ptr<RmRecord> Next() override {
        // std::cout<<"Next() of proj is called"<<std::endl;
        if(sel_idxs_.empty()){
            return nullptr;
        }
        if(is_end()){
            return nullptr;
        }
        auto &prev_cols = prev_->cols();
        auto prev_rec = prev_->Next();
        auto &proj_cols = cols_;
        auto proj_rec = std::make_unique<RmRecord>(len_);
        for (size_t proj_idx = 0; proj_idx < proj_cols.size(); proj_idx++) {
            size_t prev_idx = sel_idxs_[proj_idx];
            auto &prev_col = prev_cols[prev_idx];
            auto &proj_col = proj_cols[proj_idx];
            // std::cout<<"proj executor len"<<proj_col.len<<std::endl;
            memcpy(proj_rec->data + proj_col.offset, prev_rec->data + prev_col.offset, proj_col.len);
        }
        return proj_rec;
    }

    ast::AggregationType get_aggre_type() override {
        return prev_->get_aggre_type();
    };
    ColMeta get_col_offset(const TabCol &target) override {

    }
    // Rid &rid() override { return _abstract_rid; }
    Rid &rid() override { return _abstract_rid; }
};