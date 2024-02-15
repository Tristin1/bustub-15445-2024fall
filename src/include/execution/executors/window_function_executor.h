//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {
struct PartitionAggregateKey {
  /** The group-by values */
  std::vector<Value> group_bys_;

  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const PartitionAggregateKey &other) const -> bool {
    for (uint32_t i = 0; i < other.group_bys_.size(); i++) {
      if (group_bys_[i].CompareEquals(other.group_bys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** AggregateValue represents a value for each of the running aggregates */
struct PartitionAggregateValue {
  /** The aggregate values */
  Value aggregates_;
};

struct RankInfo {
  uint32_t rank_{0};
  uint32_t nums_{0};
  Value last_val_;
};
}  // namespace bustub

namespace std {
template <>
struct hash<bustub::PartitionAggregateKey> {
  auto operator()(const bustub::PartitionAggregateKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.group_bys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class PartitionSimpleAggregationHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  explicit PartitionSimpleAggregationHashTable(const WindowFunctionType &wind_types) : wind_types_(wind_types) {}

  /** @return The initial aggregate value for this aggregation executor */
  auto GenerateInitialAggregateValue() -> PartitionAggregateValue {
    Value value;
    switch (wind_types_) {
      case WindowFunctionType::Rank:
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        return {ValueFactory::GetIntegerValue(0)};
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        // Others starts at null.

        return {ValueFactory::GetNullValueByType(TypeId::INTEGER)};
    }
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineAggregateValues(PartitionAggregateValue *result, const PartitionAggregateValue &input) {
    switch (wind_types_) {
      case WindowFunctionType::CountStarAggregate:
      // result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
      // break;
      case WindowFunctionType::CountAggregate:
        if (result->aggregates_.IsNull()) {
          result->aggregates_ = ValueFactory::GetZeroValueByType(TypeId::INTEGER);
        }
        result->aggregates_ = result->aggregates_.Add(ValueFactory::GetIntegerValue(1));
        break;
      case WindowFunctionType::SumAggregate:
        if (result->aggregates_.IsNull()) {
          result->aggregates_ = ValueFactory::GetZeroValueByType(TypeId::INTEGER);
        }
        result->aggregates_ = result->aggregates_.Add(input.aggregates_);
        break;
      case WindowFunctionType::MinAggregate:
        if (result->aggregates_.IsNull()) {
          result->aggregates_ = input.aggregates_;
        } else {
          result->aggregates_ = result->aggregates_.Min(input.aggregates_);
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (result->aggregates_.IsNull()) {
          result->aggregates_ = input.aggregates_;
        } else {
          result->aggregates_ = result->aggregates_.Max(input.aggregates_);
        }
        break;
      case WindowFunctionType::Rank:
        if (rank_.rank_ == 0) {
          rank_.last_val_ = input.aggregates_;
          rank_.rank_ = 1;
          rank_.nums_ = 1;
        } else {
          if (rank_.last_val_.CompareEquals(input.aggregates_) == CmpBool::CmpTrue) {
            rank_.nums_++;
          } else {
            rank_.last_val_ = input.aggregates_;
            rank_.nums_++;
            rank_.rank_ = rank_.nums_;
          }
        }
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void InsertCombine(const PartitionAggregateKey &agg_key, const PartitionAggregateValue &agg_val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }

  auto Get(const PartitionAggregateKey &agg_key) -> Value {
    if (ht_.count(agg_key) != 0) {
      return ht_[agg_key].aggregates_;
    }
    return {};
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<PartitionAggregateKey, PartitionAggregateValue>::const_iterator iter)
        : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const PartitionAggregateKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const PartitionAggregateValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<PartitionAggregateKey, PartitionAggregateValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

  auto GetRank() -> Value { return ValueFactory::GetIntegerValue(rank_.rank_); }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<PartitionAggregateKey, PartitionAggregateValue> ht_{};
  /** The types of aggregations that we have */
  const WindowFunctionType &wind_types_;
  RankInfo rank_;
};

class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &partition_by)
      -> PartitionAggregateKey {
    std::vector<Value> keys;
    keys.reserve(partition_by.size());
    for (const auto &expr : partition_by) {
      keys.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple, const AbstractExpressionRef &function) -> PartitionAggregateValue {
    return {function->Evaluate(tuple, child_executor_->GetOutputSchema())};
  }

  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<Tuple> vec_;

  std::vector<std::vector<Value>> ans_;

  std::unordered_map<uint32_t, PartitionSimpleAggregationHashTable> hts_;

  uint32_t cursor_{0};
};
}  // namespace bustub
