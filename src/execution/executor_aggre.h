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
#include <vector>

class AggreExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::shared_ptr<ast::AggreClause> aggreClause_;
    std::unique_ptr<RmRecord> prev_rec = std::make_unique<RmRecord>(30);
    int len_;
    int cnt = 0;
    int int_sum = 0;
    float float_sum = 0.0;
    char* prev_num;
    std::string nickname_;
    bool flag = false;
    ColMeta col;

public:
    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return prev_->cols();
    }
    AggreExecutor(std::unique_ptr<AbstractExecutor> prev, std::shared_ptr<ast::AggreClause> aggreClause) {
        prev_ = std::move(prev);
        aggreClause_ = std::move(aggreClause);
        if(aggreClause_->aggregation_column_==nullptr) {
        } else {
            TabCol cur = {.tab_name = aggreClause_->aggregation_column_->tab_name,
                    .col_name = aggreClause_->aggregation_column_->col_name};
            col = prev_->get_col_offset(cur);
            auto tab = prev_->get_tables();
            auto cc = tab.get_col(col.name);
            col.type = cc->type;
        }
        nickname_ = std::move(aggreClause_->nickname_);
    }

    ast::AggregationType get_aggre_type() override {
        return aggreClause_->aggregation_type_;
    };
    bool has_aggre() override {
        return true;
    }
    std::string get_nickname() {
        return nickname_;
    }
    void beginTuple() override {
        prev_->beginTuple();
        if(prev_->is_end()) {
            return;
        }
        prev_rec = prev_->Next();
        if(aggreClause_->aggregation_type_==ast::AggregationType::COUNT) {
            cnt = prev_->get_block().size();
            return;
        }
        prev_num = prev_rec->data+col.offset;
        while(!prev_->is_end()) {
            switch (aggreClause_->aggregation_type_) {
                case ast::AggregationType::SUM: {
                    auto cur_rec = prev_->Next();
                    auto cur_num = cur_rec->data + col.offset;
                    if(col.type==TYPE_INT) {
                        int_sum += *(int*)(cur_num);
                        prev_rec = prev_->Next();
                    } else {
                        float_sum += *(float *)(cur_num);
                        prev_rec = prev_->Next();
                    }
                    break;
                }
                case ast::AggregationType::MAX: {
                    auto cur_rec = prev_->Next();
                    auto cur_num = cur_rec->data + col.offset;
                    auto comparison = ix_compare(cur_num,prev_num,col.type,col.len);
                    if(comparison>0) {
                        prev_num = cur_num;
                        prev_rec = std::move(cur_rec);
                    }
                    break;
                }
                case ast::AggregationType::MIN: {
                    auto cur_rec = prev_->Next();
                    auto cur_num = cur_rec->data + col.offset;
                    auto comparison = ix_compare(cur_num,prev_num,col.type,col.len);
                    if(comparison < 0) {
                        prev_num = cur_num;
                        prev_rec = std::move(cur_rec);
                    }
                    break;
                }
                default:
                    throw InternalError("Aggregation Type is not defined.");
            }
            prev_->nextTuple();
        }
    }

    [[nodiscard]] bool is_end() const override {
        return flag;
    }

    void nextTuple() override {
    }

    std::unique_ptr<RmRecord> Next() override {
        std::unique_ptr<RmRecord> ans;
        switch (aggreClause_->aggregation_type_) {
            case ast::AggregationType::COUNT: {
                char buffer[sizeof(int)]; // 创建一个足够大的缓冲区来存储float
                memcpy(buffer, &cnt, sizeof(int)); // 拷贝float的内存到buffer
                    if(cnt==0) {
                        if(!prev_->is_end())
                        prev_rec = prev_->Next();
                    }
                    // if(aggreClause_->aggregation_column_==nullptr) {
                        memcpy(prev_rec->data, buffer, sizeof(int));
                    // } else {
                    //    memcpy(prev_rec->data + col.offset, buffer, sizeof(int));
                    // }
                ans=std::move(prev_rec);
                break;
            }
            case ast::AggregationType::MAX:
            case ast::AggregationType::MIN: {
                ans = std::move(prev_rec);
                break;
            }
            case ast::AggregationType::SUM: {
                if(col.type==TYPE_INT) {
                    char buffer[sizeof(int)]; // 创建一个足够大的缓冲区来存储float
                    memcpy(buffer, &int_sum, sizeof(int)); // 拷贝float的内存到buffer
                    memcpy(prev_rec->data+col.offset, buffer, sizeof(int));
                    ans = std::move(prev_rec);
                } else {
                    char buffer[sizeof(float)]; // 创建一个足够大的缓冲区来存储float
                    memcpy(buffer, &float_sum, sizeof(float)); // 拷贝float的内存到buffer
                    memcpy(prev_rec->data+col.offset, buffer, sizeof(float));
                    ans = std::move(prev_rec);
                }
                break;
            }
        }
        flag = true;
        return ans;
    }

    std::vector<std::unique_ptr<RmRecord>> get_block() override {
        auto block = std::vector<std::unique_ptr<RmRecord>>{};
        auto record = Next();
        block.push_back(std::move(record));
        return block;
    }

    void feed(const std::map<TabCol, Value> &feed_dict) override {
        throw InternalError("Cannot feed a aggregation node");
    }
    Rid &rid() override { return _abstract_rid; }
};
