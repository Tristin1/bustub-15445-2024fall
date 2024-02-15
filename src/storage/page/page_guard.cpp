#include "storage/page/page_guard.h"
#include <cstddef>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (&that == this) {
    return;
  }
  // Drop();
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  is_removed_ = that.is_removed_;
  bpm_ = that.bpm_;
  that.page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (&that != this) {
    Drop();
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    is_removed_ = that.is_removed_;
    bpm_ = that.bpm_;
    that.page_ = nullptr;
  }
  return *this;
}

void BasicPageGuard::Drop() {
  if (!is_removed_) {
    is_removed_ = true;
    if (page_ != nullptr) {
      bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
      page_ = nullptr;
    }
  }
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  page_->RLatch();
  is_removed_ = true;
  return {bpm_, page_};
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  page_->WLatch();
  is_removed_ = true;
  return {bpm_, page_};
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  if (&that == this) {
    return;
  }
  is_removed_ = that.is_removed_;
  guard_ = std::move(that.guard_);
  that.is_removed_ = true;
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (&that != this) {
    if (!is_removed_) {
      guard_.page_->RUnlatch();
    }
    guard_ = std::move(that.guard_);
    is_removed_ = that.is_removed_;
    that.is_removed_ = true;
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (!is_removed_) {
    is_removed_ = true;
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  if (&that == this) {
    return;
  }
  is_removed_ = that.is_removed_;
  guard_ = std::move(that.guard_);
  that.is_removed_ = true;
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (&that != this) {
    if (!is_removed_) {
      guard_.page_->WUnlatch();
    }
    guard_ = std::move(that.guard_);
    is_removed_ = that.is_removed_;
    that.is_removed_ = true;
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (!is_removed_) {
    is_removed_ = true;
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
