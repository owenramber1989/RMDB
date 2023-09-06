/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
  // Todo:
  // 初始化file_handle和rid（指向第一个存放了记录的位置）
    rid_ = Rid{RM_FIRST_RECORD_PAGE, -1};
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
  // Todo:
    assert(!is_end());
    while (rid_.page_no < file_handle_->file_hdr_.num_pages) {
        RmPageHandle ph = file_handle_->fetch_page_handle(rid_.page_no);
        rid_.slot_no = Bitmap::next_bit(true, ph.bitmap, file_handle_->file_hdr_.num_records_per_page, rid_.slot_no);
        if (rid_.slot_no < file_handle_->file_hdr_.num_records_per_page) {
            return;
        }
        rid_.slot_no = -1;
        rid_.page_no++;
        file_handle_->buffer_pool_manager_->unpin_page(ph.page->get_page_id(), false);
    }
    rid_.page_no = RM_NO_PAGE;
}

/*
bool RmScan::is_end() const {
  // Todo: 修改返回值
  return rid_.slot_no == -1;
}
Rid RmScan::rid() const { return rid_; }
 */