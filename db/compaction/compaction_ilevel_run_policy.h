#pragma once

#include <iostream>
#include <vector>

#include "rocksdb/compaction_run_policy.h"

namespace ROCKSDB_NAMESPACE {

// Ilevel compaction run policy determines number of runs (tiers/files)
// a single compaction can pick from a specific level
class CompactionILevelRunPolicy : public CompactionRunPolicy {
 public:
  explicit CompactionILevelRunPolicy(std::vector<int> policy_per_level);
  const char* Name() const { return "CompactionILevelRunPolicy"; }

  int PickCompactionCount(int level) const override {
    if (level > static_cast<int>(policy_per_level_.size()) - 1) {
      return policy_per_level_[policy_per_level_.size() - 1];
    }
    return policy_per_level_[level];
  }

  void UpdateCompactionCount(int level, int count) {
    assert(level <= static_cast<int>(policy_per_level_.size()));
    if (level == static_cast<int>(policy_per_level_.size())) {
      policy_per_level_.push_back(count);
    } else {
      policy_per_level_[level] = count;
    }
  }

 private:
  std::vector<int> policy_per_level_{};
};

const CompactionRunPolicy* NewCompactionILevelRunPolicy(
    std::vector<int> policy_per_level) {
  return new CompactionILevelRunPolicy(policy_per_level);
}

}  // namespace ROCKSDB_NAMESPACE