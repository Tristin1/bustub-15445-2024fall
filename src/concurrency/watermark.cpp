#include "concurrency/watermark.h"
#include <exception>
#include <type_traits>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  // TODO(fall2023): implement me!
  current_reads_[read_ts]++;

  if (max_commit_ts_ < read_ts) {
    max_commit_ts_ = read_ts;
  }
  if (current_reads_.count(watermark_) == 0) {
    watermark_ = read_ts;
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  current_reads_[read_ts]--;
  timestamp_t i = read_ts;
  if (current_reads_[i] == 0) {
    current_reads_.erase(i);
    if (watermark_ == read_ts) {
      while (current_reads_.count(i) == 0 && i < max_commit_ts_) {
        i++;
      }
      watermark_ = i;
    }
  }
}

}  // namespace bustub
