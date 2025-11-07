#pragma once

#include <iostream>
#include <vector>

#include "rocksdb/tree_structure_policy.h"
#include <assert.h>

namespace ROCKSDB_NAMESPACE {

// Ilevel compaction run policy determines number of runs (tiers/files)
// a single compaction can pick from a specific level
class ILevelNumFileCompactTriggerPolicy : public CompactionILevelNumFileTriggerPolicy {
 public:
  explicit ILevelNumFileCompactTriggerPolicy(std::vector<double> ratio_per_level, int num_levels, int default_ratios);
  const char* Name() const { return "ILevelSizeRatioPolicy"; }

  int NumFileTrigger(int level) const override {
    if (level > static_cast<int>(NumFileTrigger_.size()) - 1) {
      return NumFileTrigger_[NumFileTrigger_.size() - 1];
    }
    return NumFileTrigger_[level];
  }

  void UpdateNumFileTrigger(int level, int ratio) {
    assert(level <= static_cast<int>(NumFileTrigger_.size()));
    if (level == static_cast<int>(NumFileTrigger_.size())) {
      NumFileTrigger_.push_back(ratio);
    } else {
      NumFileTrigger_[level] = ratio;
    }
  }

 private:
  std::vector<double> NumFileTrigger_{};
};

const CompactionILevelNumFileTriggerPolicy* NewILevelFileCompactTrigger(
    std::vector<double> size_ratio, int ilevel, int default_ratio) {
  return new ILevelNumFileCompactTriggerPolicy(size_ratio, ilevel, default_ratio);
}

}  // namespace ROCKSDB_NAMESPACE