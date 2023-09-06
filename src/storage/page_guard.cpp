// TODO(郑卯杨/ObjectNotFound)
/**
 * @brief 对缓冲池返回的page进行RAII封装，在获取时上锁，析构时解锁
 * @date 2023/6/26
*/

#include "storage/buffer_pool_manager.h"
#include "storage/page.h"
#include "storage/page_guard.h"
#include <exception>
#include <stdexcept>
// 修改移动构造函数，支持加锁
PageGuard::PageGuard(PageGuard &&that) {
    this->page_ = that.page_;
    this->bpm_ = that.bpm_;
    this->is_dirty_ = that.is_dirty_;
    that.page_ = nullptr;
    that.bpm_ = nullptr;
    that.is_dirty_ = false;

}
void PageGuard::Drop() {
    if (bpm_ != nullptr) {
        bpm_->unpin_page(get_page_id(), is_dirty_);
    }
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_ = false;
}

auto PageGuard::operator=(PageGuard &&that)  -> PageGuard & {
    if (this != &that) {
        this->page_ = that.page_;
        this->bpm_ = that.bpm_;
        this->is_dirty_ = that.is_dirty_;
        that.page_ = nullptr;
        that.bpm_ = nullptr;
        that.is_dirty_ = false;
    }
    return *this;
}

PageGuard::~PageGuard() { Drop(); };  // NOLINT

