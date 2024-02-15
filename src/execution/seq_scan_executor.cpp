//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx_->GetCatalog()->GetTable(plan->GetTableOid())->table_->MakeIterator()) {}

void SeqScanExecutor::Init() {
  while (!iter_.IsEnd()) {
    auto tuple_info = iter_.GetTuple();
    auto rid = iter_.GetRID();
    // if (tuple_info.first.is_deleted_) {
    //   ++iter_;
    //   continue;
    // }
    // if (plan_->filter_predicate_ != nullptr) {
    //   auto value = plan_->filter_predicate_->Evaluate(&tuple_info.second, GetOutputSchema());
    //   if (value.IsNull() || !value.GetAs<bool>()) {
    //     ++iter_;
    //     continue;
    //   }
    // }

    // if tuple in tableheap is most updated
    if (exec_ctx_->GetTransaction()->GetReadTs() >= tuple_info.first.ts_) {
      if (!tuple_info.first.is_deleted_) {
        if (plan_->filter_predicate_ != nullptr) {
          auto value = plan_->filter_predicate_->Evaluate(&tuple_info.second, GetOutputSchema());
          if (value.IsNull() || !value.GetAs<bool>()) {
            ++iter_;
            continue;
          }
        }
        vec_.emplace_back(std::move(tuple_info.second), rid);
      }
      ++iter_;
      // if tuple is updated by this transaction
    } else if (exec_ctx_->GetTransaction()->GetTransactionTempTs() == tuple_info.first.ts_) {
      if (tuple_info.first.is_deleted_) {
        ++iter_;
        continue;
      }
      if (plan_->filter_predicate_ != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(&tuple_info.second, GetOutputSchema());
        if (value.IsNull() || !value.GetAs<bool>()) {
          ++iter_;
          continue;
        }
      }
      vec_.emplace_back(std::move(tuple_info.second), rid);
      ++iter_;
    } else {
      std::vector<UndoLog> vec;
      auto undo_link_opt = exec_ctx_->GetTransactionManager()->GetUndoLink(rid);
      auto undo_link = undo_link_opt.value_or(UndoLink{});
      if (!undo_link.IsValid()) {
        ++iter_;
        continue;
      }

      // recover the tuple
      auto undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(undo_link);
      vec.push_back(undo_log);
      while (exec_ctx_->GetTransaction()->GetReadTs() < undo_log.ts_) {
        undo_link = undo_log.prev_version_;
        if (!undo_link.IsValid()) {
          break;
        }
        undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(undo_link);
        vec.push_back(undo_log);
      }
      if (exec_ctx_->GetTransaction()->GetReadTs() >= undo_log.ts_) {
        auto tuple_opt = ReconstructTuple(&GetOutputSchema(), tuple_info.second, tuple_info.first, vec);
        if (tuple_opt.has_value()) {
          auto tuple = tuple_opt.value();
          if (plan_->filter_predicate_ != nullptr) {
            auto value = plan_->filter_predicate_->Evaluate(&tuple, GetOutputSchema());
            if (value.IsNull() || !value.GetAs<bool>()) {
              ++iter_;
              continue;
            }
          }
          vec_.emplace_back(tuple_opt.value(), rid);
        }
      }
      ++iter_;
    }
  }
  cursor_ = 0;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == vec_.size()) {
    return false;
  }
  *tuple = vec_[cursor_].first;
  *rid = vec_[cursor_].second;
  cursor_++;
  return true;
}

}  // namespace bustub
