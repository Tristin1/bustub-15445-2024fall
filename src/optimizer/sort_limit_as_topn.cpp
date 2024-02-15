#include "common/macros.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(limit_plan.GetChildren().size() == 1, "limit plan children size not 0");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::Sort) {
      const auto sort_plan = dynamic_cast<const SortPlanNode &>(child_plan);
      return std::make_shared<TopNPlanNode>(sort_plan.output_schema_, sort_plan.children_[0], sort_plan.order_bys_,
                                            limit_plan.limit_);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
