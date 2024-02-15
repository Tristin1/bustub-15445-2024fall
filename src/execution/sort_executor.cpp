#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  cursor_ = 0;
  vec_.clear();
  while (true) {
    Tuple tuple{};
    RID rid{};
    bool flag = child_executor_->Next(&tuple, &rid);
    if (!flag) {
      break;
    }
    vec_.push_back(tuple);
  }
  std::sort(vec_.begin(), vec_.end(), [this](const Tuple &tu1, const Tuple &tu2) {
    for (const auto &node : this->plan_->order_bys_) {
      auto val1 = node.second->Evaluate(&tu1, plan_->OutputSchema());
      auto val2 = node.second->Evaluate(&tu2, plan_->OutputSchema());
      if (val1.CompareEquals(val2) != CmpBool::CmpTrue) {
        if (node.first == OrderByType::ASC || node.first == OrderByType::DEFAULT) {
          return val1.CompareLessThan(val2) == CmpBool::CmpTrue;
        }
        return val1.CompareGreaterThan(val2) == CmpBool::CmpTrue;
      }
    }
    return false;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == vec_.size()) {
    return false;
  }
  *tuple = vec_[cursor_];
  cursor_++;
  return true;
}

}  // namespace bustub
