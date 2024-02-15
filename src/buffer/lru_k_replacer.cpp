//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  lru_order_ = std::set<std::shared_ptr<LRUKNode>,
                        std::function<bool(const std::shared_ptr<LRUKNode> &p1, const std::shared_ptr<LRUKNode> &p2)>>(
      std::function<bool(const std::shared_ptr<LRUKNode> &p1, const std::shared_ptr<LRUKNode> &p2)>(
          [this](const std::shared_ptr<LRUKNode> &p1, const std::shared_ptr<LRUKNode> &p2) { return Cmp(p1, p2); }));
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> locker(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  curr_size_--;
  std::shared_ptr<LRUKNode> release = *lru_order_.begin();
  *frame_id = release->fid_;
  lru_order_.erase(release);
  node_store_.erase(*frame_id);
  // std::shared_ptr<LRUKNode> least;
  // size_t min_stamp = current_timestamp_ + 1;
  // if (!less_k_.empty()) {
  //   for (const auto &node : less_k_) {
  //     if (node->is_evictable_ && node->history_[0] < min_stamp) {
  //       min_stamp = node->history_[0];
  //       least = node;
  //     }
  //   }
  // }
  // if (min_stamp == current_timestamp_ + 1) {
  //   for (const auto &node : more_k_) {
  //     if (node->is_evictable_ && node->history_[node->history_.size() - k_] < min_stamp) {
  //       min_stamp = node->history_[node->history_.size() - k_];
  //       least = node;
  //     }
  //   }
  // }
  // least->is_evictable_ = false;
  // *frame_id = least->fid_;
  // more_k_.remove(least);
  // node_store_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> locker(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    BUSTUB_ASSERT("FRAMEIDVA", "frame id invalid");
  }
  // std::shared_ptr<LRUKNode> now_access;
  // if (node_store_.count(frame_id) == 0) {
  //   now_access = std::make_shared<LRUKNode>(frame_id);
  //   now_access->history_.push_back(current_timestamp_++);
  //   node_store_[frame_id] = now_access;
  // } else {
  //   now_access = node_store_[frame_id];
  //   lru_order_.erase(now_access);
  //   now_access->history_.push_back(current_timestamp_++);
  // }
  // lru_order_.insert(now_access);
  // if (now_access->history_.size() == k_) {
  //   less_k_.remove(now_access);
  //   more_k_.push_back(now_access);
  // } else if (now_access->history_.size() < k_) {
  //   less_k_.remove(now_access);
  //   less_k_.push_back(now_access);
  // }
  std::shared_ptr<LRUKNode> now_access;
  if (node_store_.count(frame_id) == 0) {
    auto now_access = std::make_shared<LRUKNode>(frame_id);
    node_store_[frame_id] = now_access;
    now_access->history_.push_back(current_timestamp_++);
  } else {
    now_access = node_store_[frame_id];
    if (now_access->is_evictable_) {
      lru_order_.erase(now_access);
      now_access->history_.push_back(current_timestamp_++);
      lru_order_.insert(now_access);
    } else {
      now_access->history_.push_back(current_timestamp_++);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> locker(latch_);
  if (node_store_.count(frame_id) == 0) {
    exit(1);
  }
  std::shared_ptr<LRUKNode> now_access = node_store_[frame_id];
  if (!node_store_[frame_id]->is_evictable_ && set_evictable) {
    lru_order_.insert(now_access);
    curr_size_++;
  } else if (node_store_[frame_id]->is_evictable_ && !set_evictable) {
    lru_order_.erase(now_access);
    curr_size_--;
  }
  node_store_[frame_id]->is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> locker(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  if (!node_store_[frame_id]->is_evictable_) {
    exit(1);
  }
  std::shared_ptr<LRUKNode> now_access = node_store_[frame_id];
  node_store_.erase(frame_id);
  // if (now_access->history_.size() >= k_) {
  //   more_k_.remove(now_access);
  // } else {
  //   less_k_.remove(now_access);
  // }
  lru_order_.erase(now_access);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> locker(latch_);
  return curr_size_;
}

}  // namespace bustub
