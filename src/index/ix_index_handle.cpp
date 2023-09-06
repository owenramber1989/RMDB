/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"
#include "common/config.h"
#include "defs.h"
#include "index/ix_defs.h"
#include "ix_scan.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage/page.h"
#include <cstddef>
#include <cstring>
#include <memory>
#include <mutex>
#include <utility>
#include <iostream>

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(char *target) const {
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较

    // 叶子结点从0开始，内部结点从1开始
    int l=is_leaf_page()?0:1;
    int r=page_hdr->num_key;
    while (l<r) {
        int m=(l+r)/2;
        if (ix_compare(get_key(m),target,file_hdr->col_types_,file_hdr->col_lens_)>=0) {
            r=m;
        }else {
            l=m+1;
        }
    }
    return r;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::leaf_lookup(char *key, Rid **value) {
    // Todo:
    // 1. 在叶子节点中获取目标key所在位置
    // 2. 判断目标key是否存在
    // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
    // 提示：可以调用lower_bound()和get_rid()函数。
    int begin=lower_bound(key);
    if (begin!=get_size()&& ix_compare(get_key(begin),key,file_hdr->col_types_,file_hdr->col_lens_)==0){
        *value=get_rid(begin);
        return true;
    }
    return false;
}
// 判断是否可以找到，不需要返回rid
bool IxNodeHandle::leaf_lookup(char *key){
    int begin=lower_bound(key);
//    std::cout<<"in leaf look up,page_no="<<get_page_no() <<" ,pos="<<begin<<"\n";
//    std::cout<<"key find="<<*(int*)(get_key(begin))<<"\n";
    return begin!=get_size()&& ix_compare(get_key(begin),key,file_hdr->col_types_,file_hdr->col_lens_)==0;
}
/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
PageID IxNodeHandle::internal_lookup(char *key) {
    // Todo:
    // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    // 2. 获取该孩子节点（子树）所在页面的编号
    // 3. 返回页面编号
    int r=lower_bound(key);
    // 指向了末尾，在最后一个结点中搜索
    r= r==page_hdr->num_key ?r-1:r;
    // 定位到第r个key
    auto key_at_r=get_key(r);
    // key <= ket_at_r 获取r个孩子指针 否则获取第r-1个孩子指针
    r=ix_compare(key,key_at_r,file_hdr->col_types_,file_hdr->col_lens_)>=0?r:r-1;
    // value 是指向的孩子结点的 page_id
    auto next_page_id= get_rid(r)->page_no;
    return next_page_id;
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, char *key, const Rid *rid, int n) {
    // Todo:
    // 1. 判断pos的合法性
    // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // 4. 更新当前节点的键数量
    auto size=page_hdr->num_key;
    // 插入位置不合法，直接返回
    if (pos>size||size+n>file_hdr->btree_order_) {
        return;
    }
    // 移动k-v
    memmove(get_key(pos+n), get_key(pos), file_hdr->col_tot_len_*(size-pos));
    memmove(get_rid(pos+n), get_rid(pos),sizeof(Rid)*(size-pos));
    // 插入
    memcpy(get_key(pos), key,file_hdr->col_tot_len_*n);
    memcpy(get_rid(pos), rid, sizeof(Rid)*n);
    // 更新键数量
    page_hdr->num_key+=n;
    set_dirty(true);
}
void IxNodeHandle::internal_insert_pair(int pos,char*key,Rid rid){
    internal_insert_pairs(pos,key,std::vector<Rid>{rid},1);
}
void IxNodeHandle::internal_insert_pairs(int pos,char* key,const std::vector<Rid>& rids_,int n){
    auto size=page_hdr->num_key;
    // 插入位置不合法，直接返回
    if (pos>size||size+n>file_hdr->btree_order_) {
        return;
    }
    // 移动k-v
    memmove(get_key(pos+n), get_key(pos), file_hdr->col_tot_len_*(size-pos));
    memmove(get_rid(pos+n), get_rid(pos),sizeof(Rid)*(size-pos));
    // 插入
    memcpy(get_key(pos), key,file_hdr->col_tot_len_*n);
    auto rid_start= get_rid(pos);
    for (int i = 0; i < n; ++i,++rid_start) {
        rid_start->page_no=rids_[i].page_no;
        rid_start->slot_no=rids_[i].slot_no;
    }
    // 更新键数量
    page_hdr->num_key+=n;
    set_dirty(true);
}
/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxNodeHandle::insert(char *key, const Rid &value) {
    // Todo:
    // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
    // 2. 如果key重复则不插入
    // 3. 如果key不重复则插入键值对
    // 4. 返回完成插入操作之后的键值对数量
    auto pos=lower_bound(key);
    if (pos<page_hdr->num_key&&ix_compare(key,get_key(pos),file_hdr->col_types_,file_hdr->col_lens_)==0) {
        // 重复
        return page_hdr->num_key;
    }
    insert_pair(pos, key,value);
    return page_hdr->num_key;
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    // Todo:
    // 1. 删除该位置的key
    // 2. 删除该位置的rid
    // 3. 更新结点的键值对数量
    auto size=page_hdr->num_key-pos-1;
    memmove(get_key(pos), get_key(pos+1), file_hdr->col_tot_len_*size);
    memmove(get_rid(pos), get_rid(pos+1), sizeof(Rid)*size);
    --page_hdr->num_key;
    set_dirty(true);
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
int IxNodeHandle::remove(char *key) {
    // Todo:
    // 1. 查找要删除键值对的位置
    // 2. 如果要删除的键值对存在，删除键值对
    // 3. 返回完成删除操作后的键值对数量
    int begin=lower_bound(key);
    if (begin<page_hdr->num_key&&ix_compare(key,get_key(begin),file_hdr->col_types_,file_hdr->col_lens_)==0){
        erase_pair(begin);
    }
    return page_hdr->num_key;
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    char* buf = new char[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);
    delete [] buf;
    // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁

 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
// Todo:
// 1. 获取根节点
// 2. 从根节点开始不断向下查找目标key
// 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点
// 如果根节点是叶子结点
// 使用ctx的leaf_find
void IxIndexHandle::find_leaf_page(char *key, Ctx&ctx , std::shared_ptr<Transaction> transaction)const {
    ctx.write_.emplace_back(fetch_node(ctx.root_page_id_));
    ctx.Release();
    while (!ctx.back()->is_leaf_page()) {
        auto next_page_id=ctx.back()->internal_lookup(key);
//        std::cout<<"internal node#"<<next_page_id<<"\n";
        ctx.write_.emplace_back(fetch_node(next_page_id));
        ctx.Release();
    }
}
/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::get_value(char *key, std::vector<Rid> *result, std::shared_ptr<Transaction> transaction) {
    // Todo:
    // 1. 获取目标key值所在的叶子结点
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    // 3. 把rid存入result参数中
    // 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁

    // 上锁,确保能获取根节点
    // lock();
    Ctx ctx{Operation::FIND,file_hdr_->root_page_};
    find_leaf_page(key,ctx);
    Rid* rid;
    // 叶子结点是否含有该key
    if (ctx.back()->leaf_lookup(key,&rid)) {
        result->push_back(*rid);
        return true;
    }
    return false;
}

void IxIndexHandle::split_leaf_node(Ctx&ctx){
    // 将要分裂的叶子结点作为左结点，新创建的结点作为右结点
    // 左节点的大小设置为 min_size 右节点为 max_size-min_size
    // 获取左节点
    auto left_node=std::move(ctx.back());
    ctx.pop_back();
    auto max_size=left_node->get_max_size();
    auto left_size=left_node->get_min_size();
    // 如果为奇数大小，左节点多获得一个
    left_size = max_size % 2 == 0 ? left_size : left_size + 1;
    // 创建右节点
    PageID right_id;
    auto right_node=create_node(&right_id);
    right_node->Init(true);
//    std::cout<<"in split leaf node,right node id="<<right_id<<"\n";
    // 移动左节点的键值对
    right_node->insert_pairs(0,left_node->get_key(left_size), left_node->get_rid(left_size),max_size-left_size);
    // 更新左结点的Size,右结点在Insert时已经更新了
    left_node->set_size(left_size);
    // 如果left_node是原来的last_leaf，设置file_hd
    if (file_hdr_->last_leaf_==left_node->get_page_no()) {
        file_hdr_->last_leaf_=right_id;
//        std::fstream outfile;
//        outfile.open("test.log", std::ios::out | std::ios::app);
//        outfile<<"in split leaf node set last leaf="<<right_id<<"\n";
//        outfile.close();
    }
    // 设置结点的next_page_id
    right_node->set_next_leaf(left_node->get_next_leaf());
    left_node->set_next_leaf(right_id);
    // 如果左节点为根节点，创建一个新的根节点
    if (left_node->get_page_no()==ctx.root_page_id_) {
        // 创建根节点
//        std::cout<<"in split leaf node,create new root"<<std::endl;
        PageID root_id;
        auto root_node=create_node(&root_id);
        root_node->Init(false);
        // 设置根id
        update_root_page_no(root_id);
        ctx.root_page_id_=root_id;
//        std::fstream outfile;
//        outfile.open("test.log", std::ios::out | std::ios::app);
//        outfile<<"in split leaf node set root page id="<<root_id<<"\n";
//        outfile.close();
//        std::cout<<"in split leaf node,new root node id="<<root_id<<"\n";
        // 插入左右结点
        root_node->internal_insert_pair(0, left_node->get_key(0),Rid{left_node->get_page_no(),0});
        root_node->internal_insert_pair(1, right_node->get_key(0), Rid{right_node->get_page_no(),1});
        ctx.Drop();
        return;
    }
    // 左节点的父节点也在ctx中
    // 内部结点已经达到最大值分裂
    if (ctx.back()->get_size()==ctx.back()->get_max_size()) {
        // 左节点不再使用，丢弃
        // left_node->Drop();
        left_node.reset();
        // 右节点进入队列
        ctx.write_.emplace_back(std::move(right_node));
        split_internal_node(ctx);
        return;
    }
    // 更新key,插入右结点的第一个key 和右节点的page_id
    auto key=right_node->get_key(0);
    auto pos=ctx.back()->lower_bound(key);

    ctx.back()->internal_insert_pair(pos, key,Rid{right_id,pos});
}

// TODO(ZMY) 拆分内部结点
void IxIndexHandle::split_internal_node(Ctx&ctx){
    auto right_node=std::move(ctx.back());
    ctx.pop_back();
    // 获取父亲页面
    auto node_to_update=std::move(ctx.back());
    ctx.pop_back();
    auto key=right_node->get_key(0);
    auto pos_to_insert=node_to_update->lower_bound(key);
    // 获取左右内部结点的size
    auto left_size=node_to_update->get_min_size();
    auto max_size=node_to_update->get_max_size();
    // 创建新的内部结点
    PageID new_page_id;
    auto new_node=create_node(&new_page_id);
    new_node->Init(false);
    // 移动左内部结点的键值对到右结点
//    for(int i=left_size;i<max_size;i++){
//        std::fstream outfile;
//        outfile.open("test.log", std::ios::out | std::ios::app);
//        outfile<<"#"<<i<<" : key="<<*(int*)(node_to_update->get_key(i))<<", Rid.page_no="<<node_to_update->get_rid(i)->page_no<<", Rid.slot_no="<<node_to_update->get_rid(i)<<"\n";
//        outfile.close();
//    }
    new_node->insert_pairs(0,node_to_update->get_key(left_size) , node_to_update->get_rid(left_size), max_size-left_size);
//    {
//        std::fstream outfile;
//        outfile.open("test.log", std::ios::out | std::ios::app);
//        outfile<<"\n\n\n\n";
//        outfile.close();
//    }
//    for(int i=0;i<left_size;i++){
//        std::fstream outfile;
//        outfile.open("test.log", std::ios::out | std::ios::app);
//        outfile<<"#"<<i<<" : key="<<*(int*)(new_node->get_key(i))<<", Rid.page_no="<<new_node->get_rid(i)->page_no<<", Rid.slot_no="<<new_node->get_rid(i)<<"\n";
//        outfile.close();
//    }
    node_to_update->set_size(left_size);
    // 插入 key-right_page_id
    if (pos_to_insert<=left_size) {
        // 插入到左内部结点
        pos_to_insert=node_to_update->lower_bound(key);
//        std::fstream outfile;
//        outfile.open("test.log", std::ios::out | std::ios::app);
//        outfile<<"插入到左内部结点,pos="<<pos_to_insert<<"\n";
//        outfile.close();
        node_to_update->internal_insert_pair(pos_to_insert, key, Rid{right_node->get_page_no(),pos_to_insert});
    }else {
        // 插入到新建的内部结点
        pos_to_insert=new_node->lower_bound(key);
//        std::fstream outfile;
//        outfile.open("test.log", std::ios::out | std::ios::app);
//        outfile<<"插入到新建的内部结点,pos="<<pos_to_insert<<"\n";
//        outfile.close();
        new_node->internal_insert_pair(pos_to_insert, key, Rid{right_node->get_page_no(),pos_to_insert});
    }
    // right_node使用完毕，丢弃
    right_node.reset();
    // 如果node_to_update不是根节点,将新建结点插入父节点
    if (node_to_update->get_page_no()!=ctx.root_page_id_) {
        // 分裂
        if (ctx.back()->get_size()==ctx.back()->get_max_size()) {
            // 不再使用的页面全部丢弃
            node_to_update.reset();
            ctx.write_.emplace_back(std::move(new_node));
            split_internal_node(ctx);
            ctx.Drop();
            return;
        }
        // 插入到父亲结点
        key=new_node->get_key(0);
        pos_to_insert=ctx.back()->lower_bound(key);
        ctx.back()->internal_insert_pair(pos_to_insert, key,Rid{new_page_id,pos_to_insert});
        ctx.Drop();
        return;
    }
    // 如果是根节点创建新的根节点
    PageID parent_id;
    auto parent=create_node(&parent_id);
    parent->Init(false);
    // 将左节点插入到父节点
    parent->internal_insert_pair(0, node_to_update->get_key(0),Rid{node_to_update->get_page_no(),0});
    // 将右节点插入到父节点
    key=new_node->get_key(0);
    parent->internal_insert_pair(1, key,Rid{new_page_id,1});
    // 设置根节点为新创建的结点
    update_root_page_no(parent_id);
    ctx.root_page_id_=parent_id;
//    std::fstream outfile;
//    outfile.open("test.log", std::ios::out | std::ios::app);
//    outfile<<"in split internal node set root page id="<<parent_id<<"\n";
//    outfile.close();
    ctx.Drop();
}
/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
    // 修改为返回bool 表示是否成功插入
 */
bool IxIndexHandle::insert_entry( char *key, const Rid &value, std::shared_ptr<Transaction> transaction) {
    // Todo:
    // 1. 查找key值应该插入到哪个叶子节点
    // 2. 在该叶子节点中插入键值对
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    // 提示：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁
    // lock();
//    std::cout<<"in insert entry\n";
    // 根节点是叶子结点直接插入
    auto root_id=file_hdr_->root_page_;
    // 使用Ctx管理结点
    Ctx ctx{Operation::INSERT,root_id};
    find_leaf_page(key,ctx);
    // 存在，返回false
    // ctx.write_最后一个结点就是插入的结点
    if (ctx.back()->leaf_lookup(key)) {
//        std::cout<<"key exist\n";
        return false;
    }
    // 不存在，插入
    int pos=ctx.back()->lower_bound(key);
    ctx.back()->insert_pair(pos, key, value);
//    std::cout<<"successfully insert key:"<<*(int*)(key);
//    std::cout<<"after insert,num keys="<<ctx.back()->get_size()<<"\n";
//    std::cout<<"max size="<<ctx.back()->get_max_size()<<"\n";
    if (ctx.back()->get_size()==ctx.back()->get_max_size()) {
        split_leaf_node(ctx);
    }
//    std::fstream outfile;
//    outfile.open("test.log", std::ios::out | std::ios::app);
//    outfile<<"insert key="<<*(int*)key<<" ,first leaf page="<<file_hdr_->first_leaf_<<", last leaf="<<file_hdr_->last_leaf_<<"\n";
//    outfile.close();
    return true;
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 */
bool IxIndexHandle::delete_entry(char *key, std::shared_ptr<Transaction> transaction) {
    // Todo:
    // 1. 获取该键值对所在的叶子结点
    // 2. 在该叶子结点中删除键值对
    // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁
    Ctx ctx{Operation::DELETE,file_hdr_->root_page_};
    // 如果根节点不合法，直接返回false
    find_leaf_page(key,ctx);
    auto node=std::move(ctx.back());
    ctx.pop_back();
    // 如果叶子结点中不存在，直接返回
    if (!node->leaf_lookup(key)) {
//        std::fstream outfile;
//        outfile.open("test.log", std::ios::out | std::ios::app);
//        outfile<<"in node #"<<node->get_page_no()<<"do not find key:"<<*(int*)key<<"\n";
//        outfile.close();
        return false;
    }
    // 如果对应的key已经存在，删除
    auto pos=node->lower_bound(key);
    node->erase_pair(pos);
//    std::cout<<"successfully delete key id="<<*(int*)key<<", u="<<*(float *)(key+7)<<"\n";
//    std::cout<<"successfully delete key id="<<*(int*)key<<"\n";
    // 如果叶子结点是根节点
    if (node->get_page_no()==ctx.root_page_id_) {
        if (node->get_size()==0) {
            file_hdr_->root_page_=IX_INIT_ROOT_PAGE;
            file_hdr_->first_leaf_=file_hdr_->last_leaf_=IX_INIT_ROOT_PAGE;
//            std::fstream outfile;
//            outfile.open("test.log", std::ios::out | std::ios::app);
//            outfile<<"in delete entry set last leaf="<<IX_INIT_ROOT_PAGE<<"\n";
//            outfile.close();
        }
//        std::fstream outfile;
//        outfile.open("test.log", std::ios::out | std::ios::app);
//        outfile<<"delete key="<<*(int*)key<<" ,first leaf page="<<file_hdr_->first_leaf_<<", last leaf="<<file_hdr_->last_leaf_<<"\n";
//        outfile.close();
        return true;
    }
    // 如果删除后size小于了minsize执行合并或借孩子操作
    if (node->get_size()<node->get_min_size()) {
        // 更新父节点中的key
        auto node_index=ctx.back()->find_child(node->get_page_no());
        auto right_exist = node_index + 1 < ctx.back()->get_size();
        auto left_exist = node_index != 0;
        PageID left_page_id;
        PageID right_page_id;
        bool merge_to_left = false;
        bool merge_to_right = false;
        // 能借就借，不能借再考虑合并
        // 如果有左兄弟,从左兄弟结点借
        if (left_exist) {
            left_page_id=ctx.back()->value_at(node_index-1);
            auto left_node=fetch_node(left_page_id);
            // 向左借
            if (left_node->get_size()>left_node->get_min_size()) {
                // 需要使用的页面加入队列
                ctx.write_.emplace_back(std::move(left_node));
                ctx.write_.emplace_back(std::move(node));
                leaf_borrow_left(ctx);
//                std::fstream outfile;
//                outfile.open("test.log", std::ios::out | std::ios::app);
//                outfile<<"delete key="<<*(int*)key<<" ,first leaf page="<<file_hdr_->first_leaf_<<", last leaf="<<file_hdr_->last_leaf_<<"\n";
//                outfile.close();
                return true;
            }
            merge_to_left=node->get_size()+left_node->get_size()<left_node->get_max_size();
        }
        // 从右兄弟结点借
        if (right_exist) {
            right_page_id=ctx.back()->value_at(node_index+1);
            auto right_node=fetch_node(right_page_id);
            if (right_node->get_size()>right_node->get_min_size()) {
                ctx.write_.emplace_back(std::move(right_node));
                ctx.write_.emplace_back(std::move(node));
                leaf_borrow_right(ctx);
//                std::fstream outfile;
//                outfile.open("test.log", std::ios::out | std::ios::app);
//                outfile<<"delete key="<<*(int*)key<<" ,first leaf page="<<file_hdr_->first_leaf_<<", last leaf="<<file_hdr_->last_leaf_<<"\n";
//                outfile.close();
                return true;
            }
            merge_to_right=node->get_size()+right_node->get_size()<right_node->get_max_size();
        }
        if (merge_to_left) {
            ctx.write_.emplace_back(fetch_node(left_page_id));
            ctx.write_.emplace_back(std::move(node));
            leaf_merge_left(ctx);
//            std::fstream outfile;
//            outfile.open("test.log", std::ios::out | std::ios::app);
//            outfile<<"delete key="<<*(int*)key<<" ,first leaf page="<<file_hdr_->first_leaf_<<", last leaf="<<file_hdr_->last_leaf_<<"\n";
//            outfile.close();
            return true;
        }
        if (merge_to_right) {
            // 调用merge_to_right(a,b) 等于调用merge_to_left(b,a)
            ctx.write_.emplace_back(std::move(node));
            ctx.write_.emplace_back(fetch_node(right_page_id));
            leaf_merge_left(ctx);
//            std::fstream outfile;
//            outfile.open("test.log", std::ios::out | std::ios::app);
//            outfile<<"delete key="<<*(int*)key<<" ,first leaf page="<<file_hdr_->first_leaf_<<", last leaf="<<file_hdr_->last_leaf_<<"\n";
//            outfile.close();
            return true;
        }
    }
//    std::fstream outfile;
//    outfile.open("test.log", std::ios::out | std::ios::app);
//    outfile<<"delete key="<<*(int*)key<<" ,first leaf page="<<file_hdr_->first_leaf_<<", last leaf="<<file_hdr_->last_leaf_<<"\n";
//    outfile.close();
    return true;
}


void IxIndexHandle::leaf_borrow_right(Ctx& ctx){
    // 获取结点
    auto node=std::move(ctx.back());
    ctx.pop_back();
    auto right_node=std::move(ctx.back());
    ctx.pop_back();
    // 移动
    auto old_Key=right_node->get_key(0);
    node->insert_pair(node->get_size(), old_Key, *right_node->get_rid(0));
    right_node->erase_pair(0);
    // 更新父亲结点的指针
    auto new_key=right_node->get_key(0);
    auto node_new_key=node->get_key(0);
    // 更新右结点的父亲
    auto parent_node=std::move(ctx.back());
    ctx.pop_back();
    auto index=parent_node->find_child(right_node->get_page_no());
    parent_node->set_key(index, new_key);
    // 更新node的父亲
    index=parent_node->find_child(node->get_page_no());
    parent_node->set_key(index, node_new_key);
    // Drop
    ctx.Drop();
}
void IxIndexHandle::leaf_borrow_left(Ctx& ctx){
    auto node=std::move(ctx.back());
    ctx.pop_back();
    auto left_node=std::move(ctx.back());
    ctx.pop_back();
    // 移动
    auto last=left_node->get_size()-1;
    node->insert_pair(0,left_node->get_key(last), *left_node->get_rid(last));
    left_node->erase_pair(last);
    // 更新
    auto new_key=node->get_key(0);
    // 获取父节点
    auto parent_node=std::move(ctx.back());
    ctx.pop_back();
    // 更新父节点key
    auto index=parent_node->find_child(node->get_page_no());
    parent_node->set_key(index, new_key);
    // Drop
    ctx.Drop();
}
void IxIndexHandle::leaf_merge_left(Ctx& ctx){
    // 获取结点
    auto node=std::move(ctx.back());
    ctx.pop_back();
    // 获取左兄弟结点
    auto left_node=std::move(ctx.back());
    ctx.pop_back();
    // 找到结点的父亲，从父结点中删除k-v
    auto index=ctx.back()->find_child(node->get_page_no());
    ctx.back()->erase_pair(index);
    // 移动剩余k-v到左边结点
    auto left_size=left_node->get_size();
    auto size=node->get_size();
    left_node->insert_pairs(left_size, node->get_key(0), node->get_rid(0), size);
    // 设置next_page,prev_leaf不适用，不设置
    left_node->set_next_leaf(node->get_next_leaf());
    // 右边的结点被回收,判断是不是最右结点
    if (file_hdr_->last_leaf_==node->get_page_no()) {
        file_hdr_->last_leaf_=left_node->get_page_no();
//        std::fstream outfile;
//        outfile.open("test.log", std::ios::out | std::ios::app);
//        outfile<<"in leaf merge left set last leaf="<<left_node->get_page_no()<<"\n";
//        outfile.close();
    }
    // 回收node,必须先drop才能删除
    auto page_id=node->get_page_id();
    node.reset();
    buffer_pool_manager_->delete_page(page_id);
    --file_hdr_->num_pages_;
    // 如果父亲结点的孩子数少于MinSize
    if (ctx.back()->get_size()<ctx.back()->get_min_size()) {
        left_node.reset();
        reduce_internal_node(ctx);
    }

}
void IxIndexHandle::reduce_internal_node(Ctx& ctx){
    auto node=std::move(ctx.back());
    ctx.pop_back();
    auto page_id=node->get_page_id();
    // 如果是根节点
    if (page_id.page_no==ctx.root_page_id_) {
        // 如果只有一个孩子，将孩子结点设为根节点
        if (node->get_size()==1) {
//            auto root_page_id=file_hdr_->first_leaf_;
            auto root_page_id=node->get_rid(0)->page_no;
            update_root_page_no(root_page_id);
            ctx.root_page_id_=root_page_id;
            if (root_page_id==IX_INIT_ROOT_PAGE){
                file_hdr_->first_leaf_=file_hdr_->last_leaf_=root_page_id;
            }
//            std::fstream outfile;
//            outfile.open("test.log", std::ios::out | std::ios::app);
//            outfile<<"in reduce internal node set last leaf="<<root_page_id<<" ,set root_page_id="<<root_page_id<<"\n";
//            outfile.close();
            // 回收该页面
            node.reset();
            buffer_pool_manager_->delete_page(page_id);
            --file_hdr_->num_pages_;
        }
        // 如果根节点孩子数大于1但小于min_size，不执行任何操作
        return;
    }
    // 如果不是根节点，执行借或合并操作
    auto index=ctx.back()->find_child(page_id.page_no);
    // 设置变量
    auto left_exist = index != 0;
    auto right_exist = index + 1 != ctx.back()->get_size();
    bool merge_to_right = false;
    bool merge_to_left = false;
    PageID right_page_id ;
    PageID left_page_id ;
    // 从左借
    if (left_exist) {
        left_page_id=ctx.back()->value_at(index-1);
        auto left_node=fetch_node(left_page_id);
        if (left_node->get_size()>left_node->get_min_size()) {
            ctx.write_.emplace_back(std::move(left_node));
            ctx.write_.emplace_back(std::move(node));
            internal_borrow_left(ctx);
            return;
        }
        merge_to_left=left_node->get_size()+node->get_size()<=left_node->get_max_size();
    }
    // 从右借
    if (right_exist) {
        right_page_id=ctx.back()->value_at(index+1);
        auto right_node=fetch_node(right_page_id);
        if (right_node->get_size()>right_node->get_min_size()) {
            ctx.write_.emplace_back(std::move(right_node));
            ctx.write_.emplace_back(std::move(node));
            internal_borrow_right(ctx);
            return;
        }
        merge_to_right=right_node->get_size()+node->get_size()<=right_node->get_max_size();
    }
    // 向左合并
    if (merge_to_left) {
        ctx.write_.emplace_back(fetch_node(left_page_id));
        ctx.write_.emplace_back(std::move(node));
        internal_merge_left(ctx);
        return;
    }
    if (merge_to_right) {
        ctx.write_.emplace_back(std::move(node));
        ctx.write_.emplace_back(fetch_node(right_page_id));
        internal_merge_left(ctx);
        return;
    }
}
void IxIndexHandle::internal_borrow_right(Ctx& ctx){
    // 获取结点
    auto node=std::move(ctx.back());
    ctx.pop_back();
    auto right_node=std::move(ctx.back());
    ctx.pop_back();
    auto parent_node=std::move(ctx.back());
    ctx.pop_back();
    // 移动kv到node
    node->insert_pair(node->get_size(), right_node->get_key(0), *right_node->get_rid(0));
    right_node->erase_pair(0);
    // 将right_node在父节点对应的key向下移动到page新插入的kv
    auto right_node_index=parent_node->find_child(right_node->get_page_no());
    node->set_key(node->get_size()-1, parent_node->get_key(right_node_index));
    // 将right_node的第一个key移动到父节点中
    parent_node->set_key(right_node_index, right_node->get_key(0));
    // 结束
    ctx.Drop();
}
void IxIndexHandle::internal_borrow_left(Ctx& ctx){
    // 获取结点
    auto node=std::move(ctx.back());
    ctx.pop_back();
    auto left_node=std::move(ctx.back());
    ctx.pop_back();
    auto parent_node=std::move(ctx.back());
    ctx.pop_back();
    // 移动
    auto last=left_node->get_size()-1;
    node->insert_pair(0,left_node->get_key(last), *left_node->get_rid(last));
    left_node->erase_pair(last);
    // 更新node的第一个key
    auto node_index=parent_node->find_child(node->get_page_no());
    node->set_key(1, parent_node->get_key(node_index));
    // 将node的第0个key移动到父节点中
    parent_node->set_key(node_index, node->get_key(0));
    // Drop
    ctx.Drop();
}
void IxIndexHandle::internal_merge_left(Ctx& ctx){
    // 获取结点
    auto node=std::move(ctx.back());
    ctx.pop_back();
    auto left_node=std::move(ctx.back());
    ctx.pop_back();
    // 移动
    auto left_size=left_node->get_size();
    auto size=node->get_size();
    left_node->insert_pairs(left_size, node->get_key(0), node->get_rid(0), size);
    PageID next_page_id=left_node->value_at(left_size);
    // 如果左结点孩子结点是叶子结点，设置左结点最后一个旧孩子结点的next_page
    {
        auto child_node=fetch_node(left_node->value_at(left_size-1));
        if (child_node->is_leaf_page()) {
            child_node->set_next_leaf(next_page_id);
        }
    }
    // 父节点留在队列
    // 移动父亲结点指向page的key到left_page新加入的第一个kv
    auto page_id=node->get_page_id();
    auto page_no=page_id.page_no;
    auto node_index=ctx.back()->find_child(page_no);
    left_node->set_key(left_size, ctx.back()->get_key(node_index));
    // 从父亲节点中删除
    ctx.back()->erase_pair(node_index);
    // 回收page
    node.reset();
    buffer_pool_manager_->delete_page(page_id);
    --file_hdr_->num_pages_;
    if (ctx.back()->get_size()<ctx.back()->get_min_size()) {
        left_node.reset();
        reduce_internal_node(ctx);
    }
}
/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    auto node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size()) {
        throw IndexEntryNotFoundError();
    }
    return *node->get_rid(iid.slot_no);
}

/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char *key) {

    return Iid{-1, -1};
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char *key) {

    return Iid{-1, -1};
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    if(file_hdr_->last_leaf_==INVALID_PAGE_ID){
        return {-1,0};
    }
    auto node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}
Iid IxIndexHandle::leaf_begin(char* key)const{
    Ctx ctx{Operation::FIND,file_hdr_->root_page_};
    find_leaf_page(key,ctx);
    int pos=ctx.back()->lower_bound(key);
    if(pos==ctx.back()->get_size()){
        return leaf_end();
    }
    Iid iid={ctx.back()->get_page_no(),pos};
    return iid;
}
/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */

std::unique_ptr<IxNodeHandle>IxIndexHandle::fetch_node(int page_no) const {
    // TODO(ZMY) 将Page*换成了BasicPageGuard避免手动UnpinPage
    // Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    // IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);
    auto page=buffer_pool_manager_->FetchPageBasic(PageId{fd_, page_no});
    return std::make_unique<IxNodeHandle>(file_hdr_, std::move(page));
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */

std::unique_ptr<IxNodeHandle> IxIndexHandle::create_node(int* page_no){
    file_hdr_->num_pages_++;
    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    auto page=buffer_pool_manager_->NewPageGuarded(&new_page_id);
    *page_no=new_page_id.page_no;
    return std::make_unique<IxNodeHandle>(file_hdr_,std::move(page));
}
