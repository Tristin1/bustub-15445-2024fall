//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  Column col("num", INTEGER);
  std::vector<Column> vec{col};
  schema_ = std::make_shared<const Schema>(vec);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  RID in_rid;
  Tuple in_tuple;
  while (true) {
    bool status = child_executor_->Next(&in_tuple, &in_rid);
    if (!status) {
      if (flag_) {
        return false;
      }
      flag_ = true;
      std::vector<Value> values;
      values.push_back(ValueFactory::GetIntegerValue(nums_));
      *tuple = Tuple(values, schema_.get());
      return true;
    }

    // check the tuple already in index?
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    BUSTUB_ENSURE(indexes.size() <= 1, "not only one index");
    for (auto &index_info : indexes) {
      auto attr = index_info->index_->GetKeyAttrs();
      BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
      Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
      std::vector<RID> result;
      index_info->index_->ScanKey(key, &result, exec_ctx_->GetTransaction());
      BUSTUB_ENSURE(result.size() == 1, "index more than one tuple");
      if (!result.empty()) {
        *rid = result[0];
        // if the tuple in tableheap is deleted and the deleted ts smaller than current txn read ts, than still insert
        // success
        auto tuple_info = table_info_->table_->GetTuple(*rid);
        // todo is it needed to handle the self modification case?
        if (tuple_info.first.is_deleted_ && tuple_info.first.ts_ <= exec_ctx_->GetTransaction()->GetReadTs()) {
          // get the versioninfo and lock by set in_process to true
          auto version_link_opt = exec_ctx_->GetTransactionManager()->GetVersionLink(*rid);
          if (!version_link_opt.has_value()) {
            std::unique_lock<std::shared_mutex> lock(exec_ctx_->GetTransactionManager()->version_info_mutex_);
            if (!version_link_opt.has_value()) {
              exec_ctx_->GetTransaction()->SetTainted();
              throw ExecutionException("insert conflict");
            }
            exec_ctx_->GetTransactionManager()->UpdateVersionLink(*rid, {VersionUndoLink{UndoLink{}, false}});
          }
          auto version_link = version_link_opt.value();
          if (!version_link.in_progress_) {
            std::unique_lock<std::shared_mutex> lock(exec_ctx_->GetTransactionManager()->version_info_mutex_);
            if (version_link.in_progress_) {
              exec_ctx_->GetTransaction()->SetTainted();
              throw ExecutionException("insert conflict");
            }
            version_link.in_progress_ = true;
          }
          // update the undolog chain
          std::vector<bool> modified_fields(table_info_->schema_.GetColumnCount());
          auto undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(
              UndoLog{true, modified_fields, Tuple(), tuple_info.first.ts_, version_link.prev_});
          exec_ctx_->GetTransactionManager()->UpdateUndoLink(*rid, undo_link);

          // update tableheap
          table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false},
                                                  in_tuple, *rid);
          exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, *rid);
          // releaser the versioninfo lock by set in_process to false
          version_link.in_progress_ = false;
        }
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("insert conflict");
      }
    }

    auto rid_opt =
        table_info_->table_->InsertTuple({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, in_tuple);

    if (rid_opt.has_value()) {
      *rid = rid_opt.value();
    }
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, *rid);
    for (auto &index_info : indexes) {
      auto attr = index_info->index_->GetKeyAttrs();
      BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
      Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
      bool res = index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      if (!res) {
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("insert conflict");
      }
    }
    nums_++;
  }
}

}  // namespace bustub
