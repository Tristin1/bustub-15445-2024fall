#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto GenerateHashExpr(std::vector<AbstractExpressionRef> *left_key_expressions_,
                      std::vector<AbstractExpressionRef> *right_key_expressions_, const AbstractExpressionRef &expr)
    -> bool {
  const auto *comparision_expr = dynamic_cast<ComparisonExpression *>(expr.get());
  if (comparision_expr != nullptr && comparision_expr->comp_type_ == bustub::ComparisonType::Equal) {
    const auto *column_value_expr1 = dynamic_cast<const ColumnValueExpression *>(comparision_expr->GetChildAt(0).get());
    const auto *column_value_expr2 = dynamic_cast<const ColumnValueExpression *>(comparision_expr->GetChildAt(1).get());
    if (column_value_expr1 == nullptr || column_value_expr2 == nullptr) {
      return false;
    }
    if (column_value_expr1->GetTupleIdx() == 0) {
      left_key_expressions_->push_back(comparision_expr->GetChildAt(0));
      right_key_expressions_->push_back(comparision_expr->GetChildAt(1));
    } else {
      left_key_expressions_->push_back(comparision_expr->GetChildAt(1));
      right_key_expressions_->push_back(comparision_expr->GetChildAt(0));
    }
    return true;
  }

  const auto *logic_expr = dynamic_cast<LogicExpression *>(expr.get());
  if (logic_expr != nullptr && logic_expr->logic_type_ == bustub::LogicType::And) {
    bool flag = std::all_of(expr->GetChildren().begin(), expr->GetChildren().end(),
                            [&left_key_expressions_, &right_key_expressions_](auto child_expr) {
                              return GenerateHashExpr(left_key_expressions_, right_key_expressions_, child_expr);
                            });
    // for (const auto &child_expr : expr->GetChildren()) {
    //   bool flag = GenerateHashExpr(left_key_expressions_, right_key_expressions_, child_expr);
    //   if (!flag) {
    //     return false;
    //   }
    // }
    return flag;
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nested_loop_join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    /** The expression to compute the left JOIN key */
    std::vector<AbstractExpressionRef> left_key_expressions;
    /** The expression to compute the right JOIN key */
    std::vector<AbstractExpressionRef> right_key_expressions;
    bool flag = GenerateHashExpr(&left_key_expressions, &right_key_expressions, nested_loop_join_plan.predicate_);
    if (flag) {
      return std::make_shared<HashJoinPlanNode>(optimized_plan->output_schema_, nested_loop_join_plan.GetLeftPlan(),
                                                nested_loop_join_plan.GetRightPlan(), left_key_expressions,
                                                right_key_expressions, nested_loop_join_plan.GetJoinType());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
