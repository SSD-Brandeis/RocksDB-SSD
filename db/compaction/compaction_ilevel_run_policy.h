#pragma once

#include <iostream>
#include <vector>

#include "rocksdb/compaction_run_policy.h"

namespace ROCKSDB_NAMESPACE {

// Ilevel compaction run policy determines number of runs (tiers/files)
// a single compaction can pick from a specific level
class CompactionILevelRunPolicy : public CompactionRunPolicy {
 public:
  explicit CompactionILevelRunPolicy(std::vector<int> level_compaction_granularity);
  const char* Name() const override { return "CompactionILevelRunPolicy"; }

  int PickCompactionCount(int level) const override {
    if (level > static_cast<int>(level_compaction_granularity_.size()) - 1) {
      return level_compaction_granularity_[level_compaction_granularity_.size() - 1];
    }
    return level_compaction_granularity_[level];
  }

  void UpdateCompactionCount(int level, int count) {
    assert(level <= static_cast<int>(level_compaction_granularity_.size()));
    if (level == static_cast<int>(level_compaction_granularity_.size())) {
      level_compaction_granularity_.push_back(count);
    } else {
      level_compaction_granularity_[level] = count;
    }
  }

 private:
  std::vector<int> level_compaction_granularity_{};
};

const CompactionRunPolicy* NewCompactionILevelRunPolicy(
    std::vector<int> level_compaction_granularity) {
  return new CompactionILevelRunPolicy(level_compaction_granularity);
}

const CompactionRunPolicy* NewFixedCompactionRunPolicy() {
  return new FixedCompactionRunPolicy();
}

}  // namespace ROCKSDB_NAMESPACE