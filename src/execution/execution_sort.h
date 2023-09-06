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


class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_; // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    // std::vector<ColMeta> col;
    size_t tuple_num;
    bool has_nlj = false;
    size_t off = 0;
    int limit_ = -1;
    bool has_limit = false;
    std::string tbl_name_;
    // bool is_desc_;
    std::vector<std::shared_ptr<ast::OrderBy>> orders_;
    std::vector<size_t> used_tuple;
    std::vector<std::unique_ptr<RmRecord>> all_tuples;
    // std::unique_ptr<RmRecord> current_tuple;
    std::unique_ptr<RmRecord> next_tuple;
    std::vector<std::pair<ColMeta, ast::OrderByDir>> sort_cols_;

   public:
    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
       return prev_->cols();
    }
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<std::shared_ptr<ast::OrderBy>> orders, int limit) {
        prev_ = std::move(prev);
        has_nlj = prev_->has_nlj();
        if(has_nlj) {
            // std::cout<<"\n\nnlj sort\n\n";
            off = prev_->get_sort_offset();
            tbl_name_ = prev_->get_tbl_name();
            for(const auto& order:orders){
                auto col = order->cols;
                TabCol sel_col = {.tab_name = col->tab_name,
                        .col_name = col->col_name};
                auto col_meta = prev_->get_col_offset(sel_col);
                if(col_meta.tab_name != tbl_name_) {
                    col_meta.offset += off; }
                sort_cols_.emplace_back(col_meta, order->orderby_dir);
            }
        } else {
            for (const auto &order: orders) {
                auto col = order->cols;
                TabCol sel_col = {.tab_name = col->tab_name,
                        .col_name = col->col_name};
                // cols_.push_back(prev_->get_col_offset(sel_col));
                auto col_meta = prev_->get_col_offset(sel_col);
                sort_cols_.emplace_back(col_meta, order->orderby_dir);
            }
        }
       orders_ = orders;
        tuple_num = 0;
        limit_ = limit;
        used_tuple.clear();
    }


    void beginTuple() override {
        // Get all tuples
        // std::cout<<"sort begin tuple"<<std::endl;
        if(limit_==0) {
            return;
        }
        if(limit_>0){
            has_limit = true;
        }
        prev_->beginTuple();
        while(!prev_->is_end()) {
            all_tuples.push_back(prev_->Next());
            prev_->nextTuple();
        }
        tuple_num = all_tuples.size();
        if(tuple_num==0) {
            return ;
        }
        // std::cout<<"afet sort, tuple_num is "<<tuple_num<<std::endl;
        // 对所有元组进行一次排序
        std::sort(all_tuples.begin(), all_tuples.end(),
                  [this](const std::unique_ptr<RmRecord>& a, const std::unique_ptr<RmRecord>& b) -> bool {
                      for(auto& sort_col : this->sort_cols_){
                          const char *data_a = a->data+sort_col.first.offset;
                          const char *data_b = b->data+sort_col.first.offset;
                          int comparison = ix_compare(data_a, data_b, sort_col.first.type, sort_col.first.len);
                          if(comparison != 0){
                              return sort_col.second == ast::OrderBy_DESC ? comparison < 0 : comparison > 0;
                          }
                      }
                      return false;
                  });
        // std::cout<<"there are "<<all_tuples.size()<<" tuples\n";
        // next_tuple = std::move(all_tuples.back());
        // all_tuples.erase(all_tuples.end() - 1);
    }

    [[nodiscard]] bool is_end() const override {
        if(has_limit) {
            return ((limit_<=0)||(tuple_num<=0));
        } else {
            return tuple_num <= 0;
        }
    }
    std::vector<std::unique_ptr<RmRecord>> get_block() override {
        std::reverse(all_tuples.begin(),all_tuples.end());
        if(has_limit&&all_tuples.size()>limit_) {
            all_tuples.erase(all_tuples.begin()+limit_,all_tuples.end());
        }
        return std::move(all_tuples);
    }
    void nextTuple() override {
            next_tuple = std::move(all_tuples.back());
            all_tuples.erase(all_tuples.end() - 1);
    }

    std::unique_ptr<RmRecord> Next() override {
        tuple_num--;
        limit_--;
        return std::move(next_tuple);
    }

    void feed(const std::map<TabCol, Value> &feed_dict) override {
        throw InternalError("Cannot feed a sort node");
    }
    Rid &rid() override { return _abstract_rid; }
};