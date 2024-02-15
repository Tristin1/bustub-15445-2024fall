//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
using hash_t = std::size_t;

class HashUtil2 {
 private:
  static const hash_t PRIME_FACTOR = 10000019;

 public:
  static inline auto HashBytes(const char *bytes, size_t length) -> hash_t {
    // https://github.com/greenplum-db/gpos/blob/b53c1acd6285de94044ff91fbee91589543feba1/libgpos/src/utils.cpp#L126
    hash_t hash = length;
    for (size_t i = 0; i < length; ++i) {
      hash = ((hash << 5) ^ (hash >> 27)) ^ bytes[i];
    }
    return hash;
  }

  static inline auto CombineHashes(hash_t l, hash_t r) -> hash_t {
    hash_t both[2] = {};
    both[0] = l;
    both[1] = r;
    return HashBytes(reinterpret_cast<char *>(both), sizeof(hash_t) * 2);
  }

  static inline auto SumHashes(hash_t l, hash_t r) -> hash_t {
    return (l % PRIME_FACTOR + r % PRIME_FACTOR) % PRIME_FACTOR;
  }

  template <typename T>
  static inline auto Hash(const T *ptr) -> hash_t {
    return HashBytes(reinterpret_cast<const char *>(ptr), sizeof(T));
  }

  template <typename T>
  static inline auto HashPtr(const T *ptr) -> hash_t {
    return HashBytes(reinterpret_cast<const char *>(&ptr), sizeof(void *));
  }

  /** @return the hash of the value */
  static inline auto HashValue(const Value *val) -> hash_t {
    switch (val->GetTypeId()) {
      case TypeId::TINYINT: {
        auto raw = static_cast<int64_t>(val->GetAs<int8_t>());
        return Hash<int64_t>(&raw);
      }
      case TypeId::SMALLINT: {
        auto raw = static_cast<int64_t>(val->GetAs<int16_t>());
        return Hash<int64_t>(&raw);
      }
      case TypeId::INTEGER: {
        auto raw = static_cast<int64_t>(val->GetAs<int32_t>());
        return Hash<int64_t>(&raw);
      }
      case TypeId::BIGINT: {
        auto raw = static_cast<int64_t>(val->GetAs<int64_t>());
        return Hash<int64_t>(&raw);
      }
      case TypeId::BOOLEAN: {
        auto raw = val->GetAs<bool>();
        return Hash<bool>(&raw);
      }
      case TypeId::DECIMAL: {
        auto raw = val->GetAs<double>();
        return Hash<double>(&raw);
      }
      case TypeId::VARCHAR: {
        auto raw = val->GetData();
        auto len = val->GetLength();
        return HashBytes(raw, len);
      }
      case TypeId::TIMESTAMP: {
        auto raw = val->GetAs<uint64_t>();
        return Hash<uint64_t>(&raw);
      }
      default: {
        UNIMPLEMENTED("Unsupported type.");
      }
    }
  }
};

struct HashKey {
  /** The group-by values */
  std::vector<Value> key_;
  Tuple tuple_;
  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const HashKey &other) const -> bool {
    for (uint32_t i = 0; i < other.key_.size(); i++) {
      if (key_[i].CompareEquals(other.key_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** AggregateValue represents a value for each of the running aggregates */
struct HashValue {
  /** The aggregate values */
  std::vector<Tuple> values_;
  bool joined_{false};
};
}  // namespace bustub
namespace std {
template <>
struct hash<bustub::HashKey> {
  auto operator()(const bustub::HashKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.key_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil2::CombineHashes(curr_hash, bustub::HashUtil2::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

};  // namespace std
namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto MakeLeftHashJoinKey(const Tuple *tuple) -> HashKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, plan_->GetLeftPlan()->OutputSchema()));
    }
    return {keys, *tuple};
  }

  auto MakeRightHashJoinKey(const Tuple *tuple) -> HashKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, plan_->GetRightPlan()->OutputSchema()));
    }
    return {keys, *tuple};
  }
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unordered_map<HashKey, HashValue> ht_;
  std::unordered_map<HashKey, HashValue>::const_iterator iter_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  size_t cursor_{0};
  size_t iter_cursor_{0};
  HashValue now_value_;
  Tuple right_tuple_{};
};

}  // namespace bustub
