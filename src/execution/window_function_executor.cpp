#include "execution/executors/window_function_executor.h"
#include <cstddef>
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  while (true) {
    Tuple tu{};
    RID rid{};
    bool flag = child_executor_->Next(&tu, &rid);
    if (!flag) {
      break;
    }
    vec_.push_back(tu);
  }
  for (const auto &wind_func : plan_->window_functions_) {
    if (!wind_func.second.order_by_.empty()) {
      std::sort(vec_.begin(), vec_.end(), [&wind_func, this](const Tuple &tu1, const Tuple &tu2) {
        for (const auto &node : wind_func.second.order_by_) {
          auto val1 = node.second->Evaluate(&tu1, child_executor_->GetOutputSchema());
          auto val2 = node.second->Evaluate(&tu2, child_executor_->GetOutputSchema());
          if (val1.CompareEquals(val2) != CmpBool::CmpTrue) {
            if (node.first == OrderByType::ASC || node.first == OrderByType::DEFAULT) {
              return val1.CompareLessThan(val2) == CmpBool::CmpTrue;
            }
            return val1.CompareGreaterThan(val2) == CmpBool::CmpTrue;
          }
        }
        return false;
      });
      break;
    }
  }
  ans_.reserve(vec_.size());
  for (size_t i = 0; i < vec_.size(); i++) {
    ans_.emplace_back();
  }
  for (size_t expr_num = 0; expr_num < plan_->columns_.size(); expr_num++) {
    const auto &expr = plan_->columns_[expr_num];
    const auto &colum_expr = dynamic_cast<ColumnValueExpression *>(expr.get());

    if (colum_expr->GetColIdx() != 0xffffffff) {
      for (size_t i = 0; i < vec_.size(); i++) {
        ans_[i].push_back(vec_[i].GetValue(&child_executor_->GetOutputSchema(), colum_expr->GetColIdx()));
      }
    } else {
      const auto &win_func = plan_->window_functions_.at(expr_num);

      for (size_t i = 0; i < vec_.size(); i++) {
        auto key = MakeAggregateKey(&vec_[i], win_func.partition_by_);
        auto value = MakeAggregateValue(&vec_[i], win_func.function_);
        if (hts_.count(expr_num) == 0) {
          hts_.insert({expr_num, PartitionSimpleAggregationHashTable(win_func.type_)});
        }
        if (win_func.type_ == WindowFunctionType::Rank) {
          auto value = win_func.order_by_[0].second->Evaluate(&vec_[i], child_executor_->GetOutputSchema());
          hts_.at(expr_num).InsertCombine(key, {value});
        } else {
          hts_.at(expr_num).InsertCombine(key, value);
        }
        if (!win_func.order_by_.empty()) {
          if (win_func.type_ == WindowFunctionType::Rank) {
            auto rank = hts_.at(expr_num).GetRank();
            ans_[i].push_back(rank);
          } else {
            auto val = hts_.at(expr_num).Get(key);
            ans_[i].push_back(val);
          }
        }
      }
      if (win_func.order_by_.empty()) {
        for (size_t i = 0; i < vec_.size(); i++) {
          auto key = MakeAggregateKey(&vec_[i], win_func.partition_by_);
          auto val = hts_.at(expr_num).Get(key);
          ans_[i].push_back(val);
        }
      }
    }
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == ans_.size()) {
    return false;
  }
  *tuple = Tuple(ans_[cursor_++], plan_->output_schema_.get());
  return true;
}
}  // namespace bustub
