//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple tu{};
  RID rid{};
  while (true) {
    auto flag = left_child_->Next(&tu, &rid);
    if (!flag) {
      break;
    }
    auto key = MakeLeftHashJoinKey(&tu);
    if (ht_.count(key) == 0) {
      ht_.insert({key, {}});
    }
    ht_[key].values_.push_back(tu);
  }
  iter_ = ht_.cbegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = left_child_->GetOutputSchema();
  auto right_schema = right_child_->GetOutputSchema();
  if (now_value_.values_.size() == cursor_) {
    while (true) {
      bool flag = right_child_->Next(&right_tuple_, rid);
      if (!flag) {
        while (plan_->join_type_ == JoinType::LEFT && iter_ != ht_.cend()) {
          if (!iter_->second.joined_) {
            if (iter_cursor_ < iter_->second.values_.size()) {
              std::vector<Value> vec;
              for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
                vec.push_back(iter_->second.values_[iter_cursor_].GetValue(&left_schema, i));
              }
              for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
                vec.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
              }
              auto out_schema =
                  NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan().get(), *plan_->GetRightPlan().get());
              *tuple = Tuple(vec, &out_schema);
              iter_cursor_++;
              return true;
            }
          }
          iter_cursor_ = 0;
          iter_++;
        }
        return false;
      }
      auto right_key = MakeRightHashJoinKey(&right_tuple_);
      if (ht_.count(right_key) != 0) {
        ht_[right_key].joined_ = true;
        now_value_ = ht_[right_key];
        cursor_ = 0;
        break;
      }
    }
  }
  std::vector<Value> vec;
  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    vec.push_back(now_value_.values_[cursor_].GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    vec.push_back(right_tuple_.GetValue(&right_schema, i));
  }
  cursor_++;
  auto out_schema = NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan().get(), *plan_->GetRightPlan().get());
  *tuple = Tuple(vec, &out_schema);
  return true;
}

}  // namespace bustub
