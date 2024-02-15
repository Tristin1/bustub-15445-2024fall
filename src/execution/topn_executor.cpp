#include "execution/executors/topn_executor.h"
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  cursor_ = 0;
  auto cmp = [this](const Tuple &tu1, const Tuple &tu2) {
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
  };
  std::priority_queue<Tuple, std::deque<Tuple>, std::function<bool(const Tuple &, const Tuple &)>> que(cmp);
  std::swap(que, queue_);
  vec_.clear();
  while (true) {
    Tuple tuple{};
    RID rid{};
    bool flag = child_executor_->Next(&tuple, &rid);
    if (!flag) {
      break;
    }
    if (queue_.size() < plan_->n_) {
      queue_.push(tuple);
    } else {
      if (cmp(tuple, queue_.top())) {
        queue_.pop();
        queue_.push(tuple);
      }
    }
  }
  while (!queue_.empty()) {
    vec_.push_back(queue_.top());
    queue_.pop();
  }
  std::reverse(vec_.begin(), vec_.end());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == vec_.size()) {
    return false;
  }
  *tuple = vec_[cursor_++];
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return vec_.size(); };

}  // namespace bustub
