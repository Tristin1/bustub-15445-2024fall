#include "execution/execution_common.h"
#include <vector>
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ConstructValue(const Schema *schema, const Tuple &base_tuple) -> std::vector<Value> {
  std::vector<Value> vec;
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    vec.push_back(base_tuple.GetValue(schema, i));
  }
  return vec;
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if (undo_logs.empty() && base_meta.is_deleted_) {
    return std::nullopt;
  }
  std::vector<Value> vec = ConstructValue(schema, base_tuple);
  bool flag = base_meta.is_deleted_;
  for (const auto &log : undo_logs) {
    // if (log.is_deleted_) {
    flag = log.is_deleted_;
    //}
    std::vector<Column> cols;
    for (size_t i = 0; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        cols.push_back(schema->GetColumn(i));
        // vec[i] = log.tuple_.GetValue(log.tuple_, i);
      }
    }
    auto log_schema = Schema(cols);
    size_t col_idx = 0;
    for (size_t i = 0; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        // cols.push_back(schema->GetColumn(i));
        vec[i] = log.tuple_.GetValue(&log_schema, col_idx++);
      }
    }
  }
  if (flag) {
    return std::nullopt;
  }
  return std::make_optional<Tuple>(vec, schema);
}

auto TsToString(timestamp_t ts) {
  if (ts >= TXN_START_ID) {
    return "txn" + fmt::to_string(ts - TXN_START_ID);
  }
  return fmt::to_string(ts);
}
void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println("");
  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    RID rid = iter.GetRID();
    auto tuple_info = iter.GetTuple();
    fmt::println("RID={}/{} ts={} {} tuple={} ", std::to_string(rid.GetPageId()), std::to_string(rid.GetSlotNum()),
                 TsToString(tuple_info.first.ts_), tuple_info.first.is_deleted_ ? "<del marker>" : "",
                 tuple_info.second.ToString(&table_info->schema_));
    auto undo_link = txn_mgr->GetUndoLink(rid);
    std::vector<Value> vec;
    for (size_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
      vec.push_back(tuple_info.second.GetValue(&table_info->schema_, i));
    }
    if (undo_link.has_value()) {
      if (txn_mgr->txn_map_.count(undo_link->prev_txn_) == 0) {
        ++iter;
        continue;
      }
      auto log = txn_mgr->GetUndoLog(undo_link.value());
      std::vector<Column> cols;
      for (size_t i = 0; i < log.modified_fields_.size(); i++) {
        if (log.modified_fields_[i]) {
          cols.push_back(table_info->schema_.GetColumn(i));
          // vec[i] = log.tuple_.GetValue(log.tuple_, i);
        }
      }
      auto log_schema = Schema(cols);
      size_t col_idx = 0;
      for (size_t i = 0; i < log.modified_fields_.size(); i++) {
        if (log.modified_fields_[i]) {
          // cols.push_back(schema->GetColumn(i));
          vec[i] = log.tuple_.GetValue(&log_schema, col_idx++);
        }
      }
      fmt::println("   {}@{} {} tuple={} ts={}", TsToString(undo_link->prev_txn_),
                   std::to_string(undo_link->prev_log_idx_), log.is_deleted_ ? "<del marker>" : "",
                   Tuple(vec, &table_info->schema_).ToString(&table_info->schema_), std::to_string(log.ts_));
      while (log.prev_version_.IsValid()) {
        if (txn_mgr->txn_map_.count(log.prev_version_.prev_txn_) == 0) {
          break;
        }
        log = txn_mgr->GetUndoLog(log.prev_version_);
        std::vector<Column> cols;
        for (size_t i = 0; i < log.modified_fields_.size(); i++) {
          if (log.modified_fields_[i]) {
            cols.push_back(table_info->schema_.GetColumn(i));
            // vec[i] = log.tuple_.GetValue(log.tuple_, i);
          }
        }
        auto log_schema = Schema(cols);
        size_t col_idx = 0;
        for (size_t i = 0; i < log.modified_fields_.size(); i++) {
          if (log.modified_fields_[i]) {
            // cols.push_back(schema->GetColumn(i));
            vec[i] = log.tuple_.GetValue(&log_schema, col_idx++);
          }
        }
        fmt::println("   txn{}@{} {} tuple={} ts={}", TsToString(undo_link->prev_txn_),
                     std::to_string(undo_link->prev_log_idx_), log.is_deleted_ ? "<del marker>" : "",
                     Tuple(vec, &table_info->schema_).ToString(&table_info->schema_), std::to_string(log.ts_));
      }
    }
    ++iter;
  }
  fmt::println("");
  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
