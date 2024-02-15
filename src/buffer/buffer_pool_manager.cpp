//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <future>
#include <memory>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> locker(latch_);
  // 获取pool中的空闲页面
  frame_id_t fid = GetFreeFid();
  if (fid == -1) {
    return nullptr;
  }
  Page &old_page = pages_[fid];
  // 如果为脏则写回
  if (old_page.IsDirty()) {
    FlushFrame(old_page.page_id_, fid);
  }
  page_table_.erase(pages_[fid].page_id_);
  page_id_t pid = AllocatePage();
  *page_id = pid;
  InitNewPage(pid, fid);
  return pages_ + fid;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> locker(latch_);
  // 获取pool中的空闲页面
  if (page_table_.count(page_id) != 0) {
    int fid = page_table_[page_id];
    AccessLRUK(fid);
    return pages_ + fid;
  }
  frame_id_t fid = GetFreeFid();
  if (fid == -1) {
    return nullptr;
  }
  Page &old_page = pages_[fid];
  // 先写脏页面后读新页面
  if (old_page.IsDirty()) {
    FlushFrame(old_page.page_id_, fid);
  }
  page_table_.erase(old_page.page_id_);
  InitNewPage(page_id, fid);

  DiskRequest request{false, pages_[fid].data_, page_id, disk_scheduler_->CreatePromise()};
  std::future<bool> re_callback = request.callback_.get_future();
  disk_scheduler_->Schedule(std::move(request));
  re_callback.get();

  return pages_ + fid;
}

auto BufferPoolManager::AccessLRUK(frame_id_t fid) -> void {
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  pages_[fid].pin_count_++;
}

auto BufferPoolManager::GetFreeFid() -> frame_id_t {
  frame_id_t fid = -1;
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
  } else {
    replacer_->Evict(&fid);
  }
  return fid;
}

auto BufferPoolManager::InitNewPage(page_id_t pid, frame_id_t fid) -> void {
  Page &page = pages_[fid];
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  page.page_id_ = pid;
  page.ResetMemory();
  page_table_[pid] = fid;
  AccessLRUK(fid);
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> locker(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  Page &page = pages_[page_table_[page_id]];
  if (!page.is_dirty_) {
    page.is_dirty_ = is_dirty;
  }
  if (page.pin_count_ == 0) {
    return false;
  }
  page.pin_count_--;
  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t pid) -> bool {
  std::unique_lock<std::mutex> locker(latch_);
  if (page_table_.count(pid) == 0) {
    return false;
  }
  frame_id_t fid = page_table_[pid];
  return FlushFrame(pid, fid);
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> locker(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    int pid = pages_[i].GetPageId();
    if (pid != INVALID_PAGE_ID) {
      frame_id_t fid = page_table_[pid];
      FlushFrame(pid, fid);
    }
  }
}

auto BufferPoolManager::FlushFrame(page_id_t pid, frame_id_t fid) -> bool {
  DiskRequest request{true, pages_[fid].data_, pid, disk_scheduler_->CreatePromise()};
  std::future<bool> re_callback = request.callback_.get_future();
  disk_scheduler_->Schedule(std::move(request));
  bool res = re_callback.get();
  if (res) {
    pages_[fid].is_dirty_ = false;
  }
  return res;
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> locker(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  if (pages_[page_table_[page_id]].pin_count_ != 0) {
    return false;
  }
  frame_id_t fid = page_table_[page_id];
  Page &page = pages_[fid];
  page.page_id_ = INVALID_PAGE_ID;
  page.is_dirty_ = false;
  page.pin_count_ = 0;
  page.ResetMemory();
  replacer_->Remove(fid);
  free_list_.push_back(fid);
  page_table_.erase(page_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
