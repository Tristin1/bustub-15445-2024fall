//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan->table_oid_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

// auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//   Tuple in_tuple;
//   while (true) {
//     bool next = child_executor_->Next(&in_tuple, rid);
//     if (!next) {
//       if (flag_) {
//         return false;
//       }
//       flag_ = true;
//       std::vector<Value> values;
//       values.emplace_back(ValueFactory::GetIntegerValue(nums_));
//       *tuple = Tuple(values, &GetOutputSchema());
//       return true;
//     }
//     nums_++;
//     TupleMeta meta{0, true};
//     table_info_->table_->UpdateTupleMeta(meta, *rid);
//     auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
//     for (auto &index_info : indexes) {
//       auto attr = index_info->index_->GetKeyAttrs();
//       BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
//       Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
//       index_info->index_->DeleteEntry(key, *rid, nullptr);
//     }
//   }
//   return true;
// }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple in_tuple;
  while (true) {
    bool next = child_executor_->Next(&in_tuple, rid);
    if (!next) {
      if (flag_) {
        return false;
      }
      flag_ = true;
      std::vector<Value> values;
      values.emplace_back(ValueFactory::GetIntegerValue(nums_));
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
    auto tuple_info = table_info_->table_->GetTuple(*rid);
    auto tran_temp_ts = exec_ctx_->GetTransaction()->GetTransactionTempTs();
    // log modified but not current transaction
    if (tuple_info.first.ts_ >= TXN_START_ID && tuple_info.first.ts_ != tran_temp_ts) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("w-w conflict in delete");
    }

    // record deleted by another txn
    if (tuple_info.first.ts_ < TXN_START_ID && tuple_info.first.is_deleted_) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("w-w conflict in delete");
    }

    if (tuple_info.first.ts_ < TXN_START_ID) {
      std::vector<bool> modified_fields(table_info_->schema_.GetColumnCount(), true);
      auto prev_link = exec_ctx_->GetTransactionManager()->GetUndoLink(*rid);
      auto new_undo_log =
          UndoLog{false, modified_fields, in_tuple, tuple_info.first.ts_, prev_link.value_or(UndoLink{})};
      auto undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log);
      exec_ctx_->GetTransactionManager()->UpdateUndoLink(*rid, undo_link);
    }
    // modify tableheap
    // self_modification
    nums_++;
    TupleMeta meta{tran_temp_ts, true};
    table_info_->table_->UpdateTupleMeta(meta, *rid);
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, *rid);

    // auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    // for (auto &index_info : indexes) {
    //   auto attr = index_info->index_->GetKeyAttrs();
    //   BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
    //   Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
    //   index_info->index_->DeleteEntry(key, *rid, nullptr);
    // }
  }
  return true;
}
}  // namespace bustub
