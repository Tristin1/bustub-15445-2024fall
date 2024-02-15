//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  BasicPageGuard header_guard = bpm->NewPageGuarded(&header_page_id_);
  header_page_ = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page_->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  uint32_t hash = Hash(key);
  page_id_t dir_page_id = GetDirId(hash);
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard dir_reader_guard = bpm_->FetchPageRead(dir_page_id);
  auto dir_page = dir_reader_guard.As<ExtendibleHTableDirectoryPage>();

  uint32_t bucket_index = dir_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);

  ReadPageGuard bucket_read_guard = bpm_->FetchPageRead(bucket_page_id);
  auto test = bpm_->FetchPageBasic(bucket_page_id);
  auto bucket_page = bucket_read_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  V v;
  auto res = bucket_page->Lookup(key, v, cmp_);
  if (res) {
    result->push_back(v);
  }
  return res;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetDirId(uint32_t hash) const -> page_id_t {
  uint32_t dir_index = header_page_->HashToDirectoryIndex(hash);
  return header_page_->GetDirectoryPageId(dir_index);
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  WritePageGuard page_header_guard = bpm_->FetchPageWrite(header_page_id_);
  uint32_t hash = Hash(key);
  page_id_t dir_page_id = GetDirId(hash);
  if (dir_page_id == INVALID_PAGE_ID) {
    // WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
    dir_page_id = GetDirId(hash);
    if (dir_page_id == INVALID_PAGE_ID) {
      return InsertToNewDirectory(hash, key, value);
    }
  }

  WritePageGuard dir_reader_guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = dir_reader_guard.AsMut<ExtendibleHTableDirectoryPage>();

  return InsertToBucket(dir_page, hash, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToBucket(ExtendibleHTableDirectoryPage *dir_page, uint32_t hash,
                                                       const K &key, const V &value) -> bool {
  uint32_t bucket_index = dir_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  WritePageGuard bucket_write_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  while (bucket_page->IsFull()) {
    uint32_t gd = dir_page->GetGlobalDepth();
    uint32_t ld = dir_page->GetLocalDepth(bucket_index);
    if (ld < gd) {
      uint32_t basic_index = bucket_index & dir_page->GetLocalDepthMask(bucket_index);
      uint32_t step = 1 << (ld + 1);
      page_id_t new_page_id = INVALID_PAGE_ID;
      auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);
      if (new_page_id == INVALID_PAGE_ID) {
        return false;
      }
      auto new_bucket_page = new_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      new_bucket_page->Init(bucket_max_size_);

      for (uint32_t i = basic_index; i < dir_page->Size(); i += step) {
        dir_page->SetBucketPageId(i, bucket_page_id);
        dir_page->IncrLocalDepth(i);
      }
      for (uint32_t i = basic_index + (step >> 1); i < dir_page->Size(); i += step) {
        dir_page->SetBucketPageId(i, new_page_id);
        dir_page->IncrLocalDepth(i);
      }
      MigrateEntries(bucket_page, new_bucket_page, 1 << ld);
      if (new_page_id == dir_page->GetBucketPageId(bucket_index)) {
        bucket_page = std::move(new_bucket_page);
        bucket_write_guard = new_page_guard.UpgradeWrite();
      }
    } else {
      if (gd == dir_page->GetMaxDepth()) {
        return false;
      }
      dir_page->IncrGlobalDepth();
      bucket_index = dir_page->HashToBucketIndex(hash);
    }
  }

  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(uint32_t hash, const K &key, const V &value) -> bool {
  uint32_t dir_index = header_page_->HashToDirectoryIndex(hash);
  page_id_t dir_page_id = INVALID_PAGE_ID;

  BasicPageGuard dir_write_guard = bpm_->NewPageGuarded(&dir_page_id);
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto dir_page = dir_write_guard.AsMut<ExtendibleHTableDirectoryPage>();
  dir_page->Init(directory_max_depth_);

  page_id_t bucket_page_id = INVALID_PAGE_ID;
  BasicPageGuard bucket_write_guard = bpm_->NewPageGuarded(&bucket_page_id);
  if (bucket_page_id == INVALID_PAGE_ID) {
    dir_write_guard.Drop();
    bpm_->DeletePage(dir_page_id);
    return false;
  }
  auto bucket_page = bucket_write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  header_page_->SetDirectoryPageId(dir_index, dir_page_id);
  dir_page->SetBucketPageId(0, bucket_page_id);
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t local_depth_mask) {
  for (uint32_t i = 0; i < old_bucket->Size(); i++) {
    auto &entry = old_bucket->EntryAt(i);
    auto hash = Hash(entry.first);
    if ((hash & local_depth_mask) != 0) {
      old_bucket->RemoveAt(i);
      new_bucket->Insert(entry.first, entry.second, cmp_);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // todo
  WritePageGuard page_header_guard = bpm_->FetchPageWrite(header_page_id_);
  uint32_t hash = Hash(key);
  page_id_t dir_page_id = GetDirId(hash);
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard dir_write_guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = dir_write_guard.AsMut<ExtendibleHTableDirectoryPage>();

  uint32_t bucket_index = dir_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);

  WritePageGuard bucket_wirte_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_wirte_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bool res = bucket_page->Remove(key, cmp_);
  while (bucket_page->IsEmpty()) {
    while (dir_page->CanShrink()) {
      dir_page->DecrGlobalDepth();
    }
    bucket_index = dir_page->HashToBucketIndex(hash);
    // todo
    // bucket_wirte_guard.Drop();
    // bpm_->DeletePage(bucket_page_id);
    uint32_t split_bucket_index = dir_page->GetSplitImageIndex(bucket_index);
    if (split_bucket_index == 0xffffffff) {
      bucket_wirte_guard.Drop();
      bpm_->DeletePage(bucket_page_id);

      uint32_t dir_index = header_page_->HashToDirectoryIndex(hash);
      header_page_->SetDirectoryPageId(dir_index, INVALID_PAGE_ID);
      dir_write_guard.Drop();
      bpm_->DeletePage(dir_page_id);

      return res;
    }

    uint32_t split_bucket_ld = dir_page->GetLocalDepth(split_bucket_index);
    page_id_t split_page_id = dir_page->GetBucketPageId(split_bucket_index);
    uint32_t bucket_ld = dir_page->GetLocalDepth(bucket_index);

    if (bucket_ld != split_bucket_ld) {
      break;
    }

    bucket_wirte_guard.Drop();
    bpm_->DeletePage(bucket_page_id);

    uint32_t start_bucket_index = (dir_page->GetLocalDepthMask(bucket_index) >> 1) & bucket_index;
    uint32_t step = 1 << (bucket_ld - 1);
    for (uint32_t i = start_bucket_index; i < dir_page->Size(); i += step) {
      dir_page->SetBucketPageId(i, split_page_id);
      dir_page->DecrLocalDepth(i);
    }
    auto split_page_guard = bpm_->FetchPageWrite(split_page_id);
    bucket_page = split_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_wirte_guard = std::move(split_page_guard);
    while (dir_page->CanShrink()) {
      dir_page->DecrGlobalDepth();
    }
    bucket_index = dir_page->HashToBucketIndex(hash);
  }
  // while (dir_page->CanShrink()) {
  //   dir_page->DecrGlobalDepth();
  // }
  return res;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
