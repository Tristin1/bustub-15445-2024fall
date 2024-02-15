//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>
#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/update_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

// auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//   Tuple in_tuple;
//   while (true) {
//     bool next = child_executor_->Next(&in_tuple, rid);
//     if (!next) {
//       if (flag_) {
//         return false;
//       }
//       flag_ = true;
//       std::vector<Value> values;
//       values.push_back(ValueFactory::GetIntegerValue(nums_));
//       *tuple = Tuple{values, &GetOutputSchema()};
//       return true;
//     }
//     nums_++;

//     auto del_tuple_info = table_info_->table_->GetTuple(*rid);
//     TupleMeta meta{0, true};
//     table_info_->table_->UpdateTupleMeta(meta, *rid);
//     auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
//     for (auto &index_info : indexes) {
//       auto attr = index_info->index_->GetKeyAttrs();
//       BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
//       Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
//       index_info->index_->DeleteEntry(key, *rid, nullptr);
//     }
//     std::vector<Value> values;
//     for (const auto &target_expression : plan_->target_expressions_) {
//       auto value = target_expression->Evaluate(&in_tuple, table_info_->schema_);
//       values.push_back(value);
//     }
//     Tuple new_tuple(values, &table_info_->schema_);
//     auto rid_opt = table_info_->table_->InsertTuple({0, false}, new_tuple);
//     if (!rid_opt.has_value()) {
//       return false;
//     }
//     *rid = rid_opt.value();
//     for (auto &index_info : indexes) {
//       auto attr = index_info->index_->GetKeyAttrs();
//       BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
//       Tuple key({new_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
//       index_info->index_->InsertEntry(key, *rid, nullptr);
//     }
//   }
// }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple in_tuple;
  while (true) {
    bool next = child_executor_->Next(&in_tuple, rid);
    if (!next) {
      if (flag_) {
        return false;
      }
      flag_ = true;
      std::vector<Value> values;
      values.push_back(ValueFactory::GetIntegerValue(nums_));
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    nums_++;
    auto tuple_info = table_info_->table_->GetTuple(*rid);
    auto tran_temp_ts = exec_ctx_->GetTransaction()->GetTransactionTempTs();
    // the tuple is updating by another transaction
    if (tuple_info.first.ts_ >= TXN_START_ID && tuple_info.first.ts_ != tran_temp_ts) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("w-w conflict in delete");
    }
    // the tuple is updated by  anthor transaction before this transaction start
    if (tuple_info.first.ts_ < TXN_START_ID && tuple_info.first.ts_ > exec_ctx_->GetTransaction()->GetReadTs()) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("w-w conflict in delete");
    }
    // compute new tuple
    // get old values then get new values, compare the difference to caculate the undolog
    // if self-modification update heaptable and undo log if exists;
    // else generate undo log and link together

    // get old values
    auto old_tuple_info = table_info_->table_->GetTuple(*rid);
    std::vector<Value> old_values;
    for (size_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
      old_values.push_back(old_tuple_info.second.GetValue(&table_info_->schema_, i));
    }

    // get new values
    std::vector<Value> values;
    for (const auto &target_expression : plan_->target_expressions_) {
      auto value = target_expression->Evaluate(&in_tuple, table_info_->schema_);
      values.push_back(value);
    }
    Tuple new_tuple(values, &table_info_->schema_);

    // get modified part
    std::vector<bool> modified_fields(table_info_->schema_.GetColumnCount());
    std::vector<Value> modified_values;
    std::vector<Column> modified_cols;
    for (size_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
      if (!values[i].CompareExactlyEquals(old_values[i])) {
        modified_fields[i] = true;
        modified_values.push_back(old_values[i]);
        modified_cols.push_back(table_info_->schema_.GetColumn(i));
      }
    }

    // not self modification
    auto prev_link = exec_ctx_->GetTransactionManager()->GetUndoLink(*rid);
    if (tuple_info.first.ts_ < TXN_START_ID) {
      // update undolog
      auto modified_schema = Schema(modified_cols);
      auto new_undo_log = UndoLog{false, modified_fields, Tuple(modified_values, &modified_schema),
                                  tuple_info.first.ts_, prev_link.value_or(UndoLink{})};
      auto undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log);
      exec_ctx_->GetTransactionManager()->UpdateUndoLink(*rid, undo_link);

      // update tableheap
      table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, new_tuple,
                                              *rid);
      exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, *rid);
      continue;
    }

    // self_modification
    if (!prev_link.has_value()) {
      // self insert
      table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, new_tuple,
                                              *rid);
    } else {
      // update undolog
      auto prev_undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(prev_link.value());
      auto old_modified_tuple = prev_undo_log.tuple_;

      std::vector<Column> cols;
      for (size_t i = 0; i < prev_undo_log.modified_fields_.size(); i++) {
        if (prev_undo_log.modified_fields_[i]) {
          cols.push_back(table_info_->schema_.GetColumn(i));
          // vec[i] = log.tuple_.GetValue(log.tuple_, i);
        }
      }
      auto log_schema = Schema(cols);
      std::vector<Value> replace_values;
      std::vector<bool> replace_modified_fields(prev_undo_log.modified_fields_.size());
      std::vector<Column> replace_modified_cols;
      size_t col_idx = 0;
      size_t mod_idx = 0;
      for (size_t i = 0; i < prev_undo_log.modified_fields_.size(); i++) {
        if (prev_undo_log.modified_fields_[i]) {
          replace_modified_cols.push_back(cols[col_idx]);
          replace_modified_fields[i] = true;
          replace_values.push_back(prev_undo_log.tuple_.GetValue(&log_schema, col_idx++));
          if (modified_fields[i]) {
            mod_idx++;
          }
        } else if (modified_fields[i]) {
          replace_modified_cols.push_back(modified_cols[mod_idx]);
          replace_modified_fields[i] = true;
          replace_values.push_back(modified_values[mod_idx++]);
        }
      }
      auto modified_schema = Schema(replace_modified_cols);
      auto new_undo_log = UndoLog{false, replace_modified_fields, Tuple(replace_values, &modified_schema),
                                  prev_undo_log.ts_, prev_undo_log.prev_version_};
      auto txn = exec_ctx_->GetTransactionManager()->txn_map_.at(prev_link.value().prev_txn_);
      txn->ModifyUndoLog(prev_link.value().prev_log_idx_, new_undo_log);
      table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, new_tuple,
                                              *rid);
    }
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, *rid);

    // // delete old data and old indexes
    // auto del_tuple_info = table_info_->table_->GetTuple(*rid);
    // TupleMeta meta{0, true};
    // table_info_->table_->UpdateTupleMeta(meta, *rid);
    // auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    // for (auto &index_info : indexes) {
    //   auto attr = index_info->index_->GetKeyAttrs();
    //   BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
    //   Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
    //   index_info->index_->DeleteEntry(key, *rid, nullptr);
    // }

    // // insert new data and new indexes
    // std::vector<Value> values;
    // for (const auto &target_expression : plan_->target_expressions_) {
    //   auto value = target_expression->Evaluate(&in_tuple, table_info_->schema_);
    //   values.push_back(value);
    // }
    // Tuple new_tuple(values, &table_info_->schema_);
    // auto rid_opt = table_info_->table_->InsertTuple({0, false}, new_tuple);
    // if (!rid_opt.has_value()) {
    //   return false;
    // }
    // *rid = rid_opt.value();
    // for (auto &index_info : indexes) {
    //   auto attr = index_info->index_->GetKeyAttrs();
    //   BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
    //   Tuple key({new_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
    //   index_info->index_->InsertEntry(key, *rid, nullptr);
    // }
  }
}

}  // namespace bustub
