//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID rid{};
  flag_ = false;
  left_joined_ = false;
  bool flag = left_executor_->Next(&left_tuple_, &rid);
  if (!flag) {
    flag_ = true;
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (flag_) {
    return false;
  }
  Tuple in_tuple{};
  auto &left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto &right_schema = plan_->GetRightPlan()->OutputSchema();

  while (true) {
    bool flag = right_executor_->Next(&right_tuple_, rid);
    if (!flag) {
      std::vector<Value> vec;
      if (plan_->join_type_ == JoinType::LEFT && !left_joined_) {
        for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
          vec.push_back(left_tuple_.GetValue(&left_schema, i));
        }
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
          vec.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
        }
      }
      bool left_flag = left_executor_->Next(&left_tuple_, rid);
      if (!left_flag) {
        flag_ = true;
      } else {
        right_executor_->Init();
      }
      if (plan_->join_type_ == JoinType::LEFT && !left_joined_) {
        auto out_schema =
            NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan().get(), *plan_->GetRightPlan().get());
        *tuple = Tuple(vec, &out_schema);
        return true;
      }
      left_joined_ = false;
      if (flag_) {
        return false;
      }
      right_executor_->Next(&right_tuple_, rid);
    }
    auto value = plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema, &right_tuple_, right_schema);
    if (!value.IsNull() && value.GetAs<bool>()) {
      left_joined_ = true;
      std::vector<Value> vec;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
        vec.push_back(left_tuple_.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
        vec.push_back(right_tuple_.GetValue(&right_schema, i));
      }
      auto out_schema =
          NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan().get(), *plan_->GetRightPlan().get());
      *tuple = Tuple(vec, &out_schema);
      return true;
    }
  }
}

}  // namespace bustub
