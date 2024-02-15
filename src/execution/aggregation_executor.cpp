//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  aht_.Clear();
  while (true) {
    auto flag = child_executor_->Next(&tuple, &rid);
    if (!flag) {
      break;
    }
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (aht_.Begin() == aht_.End() && !flag_ && plan_->group_bys_.empty()) {
      flag_ = true;
      //   for (const auto &value : aht_iterator_.Key().group_bys_) {
      //     res.push_back(value);
      //   }
      auto values = aht_.GenerateInitialAggregateValue();
      std::vector<Value> res;
      res.reserve(values.aggregates_.size());
      for (const auto &value : values.aggregates_) {
        res.push_back(value);
      }
      *tuple = Tuple(res, &GetOutputSchema());
      return true;
    }
    return false;
  }
  std::vector<Value> res;
  for (const auto &value : aht_iterator_.Key().group_bys_) {
    res.push_back(value);
  }
  for (const auto &value : aht_iterator_.Val().aggregates_) {
    res.push_back(value);
  }
  *tuple = Tuple(res, &GetOutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
