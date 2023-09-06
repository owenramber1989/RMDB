#pragma once
// TODO(郑卯杨/ObjectNotFound)
/** 
 * @brief 对缓冲池返回的page进行RAII封装，在获取时上锁，析构时解锁
 * @date 2023/6/26   
*/
#include "common/config.h"
#include "storage/page.h"
class BufferPoolManager;

class PageGuard {
public:
    PageGuard() = default;

    PageGuard(BufferPoolManager *bpm, Page *page) : bpm_(bpm), page_(page) {}
    // 禁用拷贝赋值和拷贝构造
    PageGuard(const PageGuard &) = delete;
    auto operator=(const PageGuard &) -> PageGuard & = delete;
    PageGuard(PageGuard &&that) ;

    /**
     * @brief Drop a page guard
     * 当不再使用时主动丢弃页面
     */
    void Drop();

    auto operator=(PageGuard &&that)  -> PageGuard &;

    /**
     * @brief Destructor for BasicPageGuard
     * 当Page生命周期结束时解锁或释放Page
     */
    ~PageGuard();

    auto get_page_id() -> PageId {
        return page_->get_page_id();
    }

    auto get_data() -> char * {
        is_dirty_ = true;
        return page_->get_data();
    }

    inline void set_dirty(bool is_dirty){page_->set_dirty(is_dirty);}
private:
    BufferPoolManager *bpm_{nullptr};
    Page *page_{nullptr};
    bool is_dirty_{false};
};