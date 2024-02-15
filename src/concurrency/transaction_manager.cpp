//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);
  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_.load() + 1;
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  for (const auto &[table_id, rids] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_id);
    for (auto rid : rids) {
      auto tuple_info = table_info->table_->GetTuple(rid);
      table_info->table_->UpdateTupleInPlace({commit_ts, tuple_info.first.is_deleted_}, tuple_info.second, rid);
    }
  }
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = commit_ts;
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  last_commit_ts_++;
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // get all tables through catalog
  auto table_names = catalog_->GetTableNames();
  // iterate the tables , get the iterator of the table
  std::unordered_set<txn_id_t> sets;
  auto water_mark = running_txns_.GetWatermark();
  // through the iterator get every tuple and traverse the version chain utill the ts < watermark, add every txnid to
  // set
  for (const auto &name : table_names) {
    auto table_info = catalog_->GetTable(name);
    for (auto iter = table_info->table_->MakeIterator(); !iter.IsEnd(); ++iter) {
      RID rid = iter.GetRID();
      auto tuple_info = table_info->table_->GetTuple(rid);
      if (tuple_info.first.ts_ <= water_mark) {
        continue;
      }
      auto undo_link_opt = GetUndoLink(rid);
      if (undo_link_opt.has_value()) {
        auto undo_link = undo_link_opt.value();
        sets.insert(undo_link.prev_txn_);
        auto undo_log = GetUndoLog(undo_link);
        while (undo_log.ts_ > water_mark) {
          undo_link = undo_log.prev_version_;
          if (!undo_link.IsValid()) {
            break;
          }
          undo_log = GetUndoLog(undo_link);
          sets.insert(undo_link.prev_txn_);
        }
      }
    }
  }

  std::unordered_set<txn_id_t> temp;
  for (const auto &[txn_id, txn_ptr] : txn_map_) {
    if (sets.count(txn_id) == 0 && txn_ptr->GetTransactionState() != TransactionState::RUNNING &&
        txn_ptr->GetTransactionState() != TransactionState::TAINTED) {
      temp.insert(txn_id);
    }
  }
  // delete all txn not in the set
  for (auto txn_id : temp) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub
