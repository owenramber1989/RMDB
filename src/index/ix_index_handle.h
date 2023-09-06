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


#include "common/config.h"
#include "common/context.h"
#include "ix_defs.h"
#include "storage/page.h"
#include "storage/page_guard.h"
#include "transaction/transaction.h"
#include <deque>
#include <memory>
#include <unordered_map>


enum class Operation { FIND = 0, INSERT, DELETE };  // 三种操作：查找、插入、删除

static const bool binary_search = false;

inline int ix_compare(const char *a, const char *b, ColType type, int col_len) {
    switch (type) {
        case TYPE_INT: {
            int ia = *(int *)a;
            int ib = *(int *)b;
            // std::cout<<"ia is "<<ia<<" and ib is "<<ib<<std::endl;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case TYPE_FLOAT: {
            float fa = *(float *)a;
            float fb = *(float *)b;
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case TYPE_BIGINT: {
            std::int64_t ba = *(std::int64_t *)a;
            std::int64_t bb = *(std::int64_t *)b;
            return (ba < bb) ? -1 : ((ba > bb) ? 1 : 0);
        }
        case TYPE_DATETIME:
            return memcmp(a,b,col_len);
        case TYPE_STRING:
            return memcmp(a, b, col_len);
        default:
            throw InternalError("Unexpected data type");
    }
}

inline int ix_compare(const char* a, const char* b, const std::vector<ColType>& col_types, const std::vector<int>& col_lens) {
    int offset = 0;
    for(size_t i = 0; i < col_types.size(); ++i) {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if(res != 0) return res;
        offset += col_lens[i];
    }
    return 0;
}

/* 管理B+树中的每个节点 */
class IxNodeHandle {
    friend class IxIndexHandle;
    friend class IxScan;

   private:
    const IxFileHdr *file_hdr;      // 节点所在文件的头部信息
    // Page *page;                     // 存储节点的页面
    PageGuard page;
    // 存储节点的页面
    IxPageHdr *page_hdr;            // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
    char *keys;                     // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
    Rid *rids;                      // page->data的第三部分，指针指向首地址

   public:
    IxNodeHandle() = default;

    // TODO(ZMY)将Page*换成了BasicPageGuard避免手动UnpinPage
    IxNodeHandle(const IxFileHdr *file_hdr_, PageGuard&& page_) : file_hdr(file_hdr_), page(std::move(page_)) {
        page_hdr = reinterpret_cast<IxPageHdr *>(page.get_data());
        keys = page.get_data() + sizeof(IxPageHdr);
        rids = reinterpret_cast<Rid *>(keys + file_hdr->keys_size_);
    }

    inline void set_dirty(bool is_dirty){page.set_dirty(is_dirty);}
    // 添加初始化函数
    void Init(bool is_leaf){
        page_hdr->num_key=0;
        page_hdr->is_leaf=is_leaf;
        page_hdr->prev_leaf=page_hdr->next_leaf=INVALID_PAGE_ID;
    }
    int get_size() { return page_hdr->num_key; }

    void set_size(int size) { page_hdr->num_key = size; }

    int get_max_size() { return file_hdr->btree_order_ ; }
    // 修改get_min_size
    int get_min_size() {
        if (!is_leaf_page()) {
            // 内部结点
            return (get_max_size()+1)/2;
        }
        return get_max_size() / 2;
    }
    int key_at(int i) { return *(int *)get_key(i); }

    /* 得到第i个孩子结点的page_no */
    PageID value_at(int i) { return get_rid(i)->page_no; }

    PageID get_page_no() { return page.get_page_id().page_no; }

    PageId get_page_id() { return page.get_page_id(); }

    PageID get_next_leaf() { return page_hdr->next_leaf; }

    PageID get_prev_leaf() { return page_hdr->prev_leaf; }

    PageID get_parent_page_no() { return page_hdr->parent; }

    bool is_leaf_page()const { return page_hdr->is_leaf; }

    bool is_root_page() { return get_parent_page_no() == INVALID_PAGE_ID; }

    void set_next_leaf(PageID page_no) { page_hdr->next_leaf = page_no; }

    void set_prev_leaf(PageID page_no) { page_hdr->prev_leaf = page_no; }

    void set_parent_page_no(PageID parent) { page_hdr->parent = parent; }

    char *get_key(int key_idx) const { return keys + key_idx * file_hdr->col_tot_len_; }

    Rid *get_rid(int rid_idx) const { return &rids[rid_idx]; }

    void set_key(int key_idx, const char *key) { memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_); set_dirty(true);}

    void set_rid(int rid_idx, const Rid &rid) { rids[rid_idx] = rid; set_dirty(true);}

    int lower_bound(char *target) const;

    void insert_pairs(int pos, char *key, const Rid *rid, int n);

    PageID internal_lookup(char *key);

    bool leaf_lookup(char *key, Rid **value);
    bool leaf_lookup(char *key);

    int insert(char *key, const Rid &value);

    // 用于在结点中的指定位置插入单个键值对
    void insert_pair(int pos, char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); }
    void internal_insert_pair(int pos,char*key,Rid rid);
    void internal_insert_pairs(int pos,char* key,const std::vector<Rid>& rids_,int n);
    void erase_pair(int pos);

    int remove(char *key);

    /**
     * @brief used in internal node to remove the last key in root node, and return the last child
     *
     * @return the last child
     */
    PageID remove_and_return_only_child() {
        assert(get_size() == 1);
        PageID child_page_no = value_at(0);
        erase_pair(0);
        assert(get_size() == 0);
        return child_page_no;
    }

    /**
     * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
     * @param child
     * @return int
     */
    int find_child(PageID child_id) {
        int rid_idx;
        for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
            if (get_rid(rid_idx)->page_no == child_id) {
                break;
            }
        }
        assert(rid_idx < page_hdr->num_key);
        return rid_idx;
    }

};

class Ctx;
/* B+树 */
class IxIndexHandle {
    friend class IxScan;
    friend class IxManager;

private:
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
    int fd_;                                    // 存储B+树的文件
    IxFileHdr* file_hdr_;                       // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
    std::mutex root_latch_;

public:
    IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);
    ~IxIndexHandle(){delete file_hdr_;}
    // for search
    bool get_value(char *key, std::vector<Rid> *result, std::shared_ptr<Transaction> transaction);
    // 修改定义，不需要返回根节点是否加锁
    void find_leaf_page(char *key, Ctx&ctx , std::shared_ptr<Transaction>transaction=nullptr)const;
    // for insert
    bool insert_entry(char *key, const Rid &value, std::shared_ptr<Transaction>transaction);

    // 使用自定义的split函数
    void split_leaf_node(Ctx&ctx);
    void split_internal_node(Ctx&ctx);

    // for delete
    bool delete_entry(char *key, std::shared_ptr<Transaction> transaction);
    Iid lower_bound(const char *key);

    Iid upper_bound(const char *key);

    Iid leaf_end() const;

    Iid leaf_begin() const;

    Iid leaf_begin(char* key)const;

    inline void lock(){root_latch_.lock();}
    inline void unlock(){root_latch_.unlock();}
    PageID get_root_page_id(){return file_hdr_->root_page_;};
    PageID get_first_leaf_id(){return file_hdr_->first_leaf_;};
    PageID get_last_leaf_id(){return file_hdr_->last_leaf_;};
    int get_max_size(){return file_hdr_->btree_order_;}
    // 自定义函数
    void reduce_internal_node(Ctx& ctx);
    void leaf_borrow_right(Ctx& ctx);
    void leaf_borrow_left(Ctx& ctx);
    void leaf_merge_left(Ctx& ctx);
    void internal_borrow_right(Ctx& ctx);
    void internal_borrow_left(Ctx& ctx);
    void internal_merge_left(Ctx& ctx);
    DiskManager* get_disk_mgr(){return disk_manager_;}
    IxFileHdr* get_ix_file_hdr(){return file_hdr_;}
    int get_fd(){return fd_;}
private:
    // 辅助函数
    void update_root_page_no(PageID root) { file_hdr_->root_page_ = root; }
    void update_first_leaf(PageID first_leaf){file_hdr_->first_leaf_=first_leaf;}
    void update_last_leaf(PageID last_leaf){file_hdr_->last_leaf_=last_leaf;}
    bool is_empty() const { return file_hdr_->root_page_ == IX_INIT_ROOT_PAGE && file_hdr_->first_leaf_==IX_INIT_ROOT_PAGE; }

    // 使用智能指针进行内存管理
    std::unique_ptr<IxNodeHandle> fetch_node(int page_no)const;
    std::unique_ptr<IxNodeHandle> create_node(int* page_no);

    // for index test
    Rid get_rid(const Iid &iid) const;
};

// TODO(郑卯杨) 添加自定义上下文类 来记录加锁过程
class Ctx{
public:
    Ctx()=default;
    // 记录执行的操作
    Operation opt;
    // 保存根结点ID
    PageID root_page_id_{INVALID_PAGE_ID};
    // 节点管理
    std::deque<std::unique_ptr<IxNodeHandle>> write_;
    // 函数
    bool is_root_page(PageID page_id){return page_id==root_page_id_;}
    decltype(auto) back(){return write_.back();}
    inline void pop_back(){write_.pop_back();};
    // 释放所有结点
    void Drop(){
        write_.clear();
    }

    // 释放不需要的结点
    void Release(){
        auto release=[&](){
            write_.erase(write_.begin(),--write_.end());
        };
        auto size=write_.size();
        if(size==0){return;}
        // 搜索时释放
        if (opt==Operation::FIND) {
            release();
        }
        // 插入时
        if (opt==Operation::INSERT) {
            // 如果write_set最后一个加入的结点不会分裂，释放父节点
            // 叶子结点当前size+1应小于maxsize(<)
            if (write_.back()->is_leaf_page()) {
                if (write_.back()->get_size()+1<write_.back()->get_max_size()) {
                    release();
                }
                return;
            }
            // 内部结点当前size+1应小于等于maxsize(<=)
            if (write_.back()->get_size()+1<=write_.back()->get_max_size()) {
                release();
            }
        }
        if (opt==Operation::DELETE) {
            // 删除时释放，当前size>=minsize+1
            if (write_.back()->get_size()-1>=write_.back()->get_min_size()) {
                release();
            }
        }

    }
};

