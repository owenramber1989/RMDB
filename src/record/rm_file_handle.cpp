/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid &rid,
                                                   Context *context) const {
  // Todo:
  // 1. 获取指定记录所在的page handle
  // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）

    auto record = std::make_unique<RmRecord>(file_hdr_.record_size);
    RmPageHandle ph = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(ph.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    char *slot = ph.get_slot(rid.slot_no);
    memcpy(record->data, slot, file_hdr_.record_size);
    record->size = file_hdr_.record_size;
    buffer_pool_manager_->unpin_page({fd_,rid.page_no}, false);
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char *buf, Context *context) {
    RmPageHandle ph = create_page_handle();
    // get slot number
    int slot_no = Bitmap::first_bit(false, ph.bitmap, file_hdr_.num_records_per_page);
    assert(slot_no < file_hdr_.num_records_per_page);
    // update bitmap
    Bitmap::set(ph.bitmap, slot_no);
    // update page header
    ph.page->set_dirty(true);
    // if(context!= nullptr)
    // ph.page->set_page_lsn(context->log_mgr_->GetNextLsn());
    ph.page_hdr->num_records++;
    if (ph.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // page is full
        file_hdr_.first_free_page_no = ph.page_hdr->next_free_page_no;
    }
    // copy record data into slot
    char *slot = ph.get_slot(slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    Rid rid{ph.page->get_page_id().page_no, slot_no};
    buffer_pool_manager_->unpin_page({fd_,rid.page_no}, true);
    return rid;
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid &rid, char *buf) {
  RmPageHandle page_handle = fetch_page_handle(rid.page_no);

  // 确保插槽是空的
  assert(!Bitmap::is_set(page_handle.bitmap, rid.slot_no));

  // 获取插槽地址并复制记录数据
  char *record_slot = page_handle.get_slot(rid.slot_no);
  memcpy(record_slot, buf, file_hdr_.record_size);

  // 更新bitmap和元数据
  Bitmap::set(page_handle.bitmap, rid.slot_no);
  page_handle.page_hdr->num_records++;
  // 检查页面是否已满
  if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
    // 找到下一个有空闲插槽的页面
    create_new_page_handle();
    file_hdr_.first_free_page_no =
        Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_pages);
  }
    buffer_pool_manager_->unpin_page({fd_,rid.page_no}, true);

}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid &rid, Context *context) {
  // Todo:
  // 1. 获取指定记录所在的page handle
  // 2. 更新page_handle.page_hdr中的数据结构
  // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
  RmPageHandle page_handle = fetch_page_handle(rid.page_no);

  // 确保该记录存在
  assert(Bitmap::is_set(page_handle.bitmap, rid.slot_no));

  // 清除bitmap中对应的位
  Bitmap::reset(page_handle.bitmap, rid.slot_no);
  // memset(page_handle.get_slot(rid.slot_no),0,file_hdr_.record_size);
   // if(context!= nullptr)
  // page_handle.page->set_page_lsn(context->log_mgr_->GetNextLsn());
  // 检查页面是否已从满变为不满
  if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
    // 更新空闲页面列表
    release_page_handle(page_handle);
  }
    page_handle.page_hdr->num_records--;
  buffer_pool_manager_->unpin_page({fd_,rid.page_no},true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid &rid, char *buf, Context *context) {
  // Todo:
  // 1. 获取指定记录所在的page handle
  // 2. 更新记录
  RmPageHandle page_handle = fetch_page_handle(rid.page_no);

  // 确保该记录存在
  assert(Bitmap::is_set(page_handle.bitmap, rid.slot_no));
  // if(context!= nullptr)
  // page_handle.page->set_page_lsn(context->log_mgr_->GetNextLsn());
  // 获取插槽地址并复制新的记录数据
  char *record_slot = page_handle.get_slot(rid.slot_no);
  memcpy(record_slot, buf, file_hdr_.record_size);
  buffer_pool_manager_->unpin_page({fd_,rid.page_no}, true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
 */
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
  // Todo:
  // 使用缓冲池获取指定页面，并生成page_handle返回给上层
  // if page_no is invalid, throw PageNotExistError exception
  if (page_no == INVALID_PAGE_ID || page_no > file_hdr_.num_pages ) {
    throw PageNotExistError("table", page_no);
  }
  // 使用缓冲池管理器获取指定页面
  Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});

  if (page == nullptr) {
    throw PageNotExistError("table", page_no);
  }

  // 返回相应的RmPageHandle对象
  return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
  // Todo:
  // 1.使用缓冲池来创建一个新page
  // 2.更新page handle中的相关信息
  // 3.更新file_hdr_
  // PageId* pageId = nullptr;
  // pageId->page_no = file_hdr_.num_pages++;
  PageId PageId = {GetFd(), -1};
  Page *new_page = buffer_pool_manager_->new_page(&PageId);
  /*
  if (new_page == nullptr) {
      throw NoFreePageError();
  }
  */
  // 更新文件头的空闲页面信息
  file_hdr_.first_free_page_no = new_page->get_page_id().page_no;
  file_hdr_.num_pages += 1;

  // 更新新页面的元数据
  RmPageHandle page_handle(&file_hdr_, new_page);
  page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
  page_handle.page_hdr->num_records = 0;
  Bitmap::init(page_handle.bitmap,page_handle.file_hdr->bitmap_size);
  return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
  // Todo:
  // 1. 判断file_hdr_中是否还有空闲页
  //     1.1
  //     没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
  //     1.2 有空闲页：直接获取第一个空闲页
  // 2. 生成page handle并返回给上层

  RmPageHandle page_handle;

  if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
    page_handle = create_new_page_handle();
    return page_handle;
  }

  PageId pageId{fd_, file_hdr_.first_free_page_no};
  // std::cout<<"in create_page_handle: "<<pageId.toString()<<"\n";
  return RmPageHandle(&file_hdr_,buffer_pool_manager_->fetch_page(pageId));
}

/**
 * @description:
 * 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle &page_handle) {
  // Todo:
  // 当page从已满变成未满，考虑如何更新：
  // 1. page_handle.page_hdr->next_free_page_no
  // 2. file_hdr_.first_free_page_no
  // 当page从已满变成未满时，更新相关元数据

  // 检查当前页面是否为满页
  if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
    // 当前页面是满页，更新相关元数据

    // 更新页面头部的下一个空闲页面号
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;

    // 更新文件头部的第一个空闲页面号
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
    // std::cout<<"in release page handle: "<<file_hdr_.first_free_page_no<<"\n";
  }
}
