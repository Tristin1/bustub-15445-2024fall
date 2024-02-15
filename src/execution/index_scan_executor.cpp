//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/index/extendible_hash_table_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (vis_) {
    return false;
  }
  vis_ = true;

  // get the index key
  Schema dummy_schema({});
  auto key = plan_->pred_key_->Evaluate(nullptr, dummy_schema);
  auto idx = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  Tuple key_tu({key}, &idx->key_schema_);

  // get the results in table heap
  std::vector<RID> result;
  idx->index_->ScanKey(key_tu, &result, exec_ctx_->GetTransaction());
  if (result.empty()) {
    return false;
  }
  *rid = result[0];
  auto tuple_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(*rid);

  // tuple in table heap is most updated
  if (exec_ctx_->GetTransaction()->GetReadTs() >= tuple_info.first.ts_) {
    if (tuple_info.first.is_deleted_) {
      return false;
    }
    *tuple = tuple_info.second;
    return true;
  }

  // if tuple in table heap is updated by this transaction
  if (exec_ctx_->GetTransaction()->GetTransactionTempTs() == tuple_info.first.ts_) {
    if (tuple_info.first.is_deleted_) {
      return false;
    }
    *tuple = tuple_info.second;
    return true;
  }

  std::vector<UndoLog> vec;
  auto undo_link_opt = exec_ctx_->GetTransactionManager()->GetUndoLink(*rid);
  auto undo_link = undo_link_opt.value_or(UndoLink{});
  if (!undo_link.IsValid()) {
    return false;
  }

  // recover the tuple
  // get the undo log before
  auto undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(undo_link);
  vec.push_back(undo_log);
  while (exec_ctx_->GetTransaction()->GetReadTs() < undo_log.ts_) {
    undo_link = undo_log.prev_version_;
    if (!undo_link.IsValid()) {
      return false;
    }
    undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(undo_link);
    vec.push_back(undo_log);
  }

  // recover the tuple
  auto tuple_opt = ReconstructTuple(&GetOutputSchema(), tuple_info.second, tuple_info.first, vec);
  if (tuple_opt.has_value()) {
    *tuple = tuple_opt.value();
    return true;
  }
  return false;
}

}  // namespace bustub
