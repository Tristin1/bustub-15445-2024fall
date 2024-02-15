#include <cstddef>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeOrderByAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_.get() == nullptr) {
      return plan;
    }
    const auto *comparision_expr = dynamic_cast<ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
    if (comparision_expr == nullptr || comparision_expr->comp_type_ != bustub::ComparisonType::Equal) {
      return optimized_plan;
    }
    const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(comparision_expr->GetChildAt(0).get());
    ConstantValueExpression *constant_expr;
    if (column_value_expr == nullptr) {
      column_value_expr = dynamic_cast<const ColumnValueExpression *>(comparision_expr->GetChildAt(1).get());
      constant_expr = dynamic_cast<ConstantValueExpression *>(comparision_expr->GetChildAt(0).get());
    } else {
      constant_expr = dynamic_cast<ConstantValueExpression *>(comparision_expr->GetChildAt(1).get());
    }
    std::vector<uint32_t> order_by_column_ids;
    order_by_column_ids.push_back(column_value_expr->GetColIdx());

    const auto *table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
    const auto indices = catalog_.GetTableIndexes(table_info->name_);

    for (const auto *index : indices) {
      const auto &columns = index->index_->GetKeyAttrs();
      if (order_by_column_ids == columns) {
        return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_, index->index_oid_,
                                                   seq_scan_plan.filter_predicate_, constant_expr);
      }
    }
  }
  return plan;
}

}  // namespace bustub
