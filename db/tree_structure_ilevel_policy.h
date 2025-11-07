#pragma once

#include <iostream>
#include <vector>

#include "rocksdb/tree_structure_policy.h"
#include <assert.h>

namespace ROCKSDB_NAMESPACE {

// Ilevel compaction run policy determines number of runs (tiers/files)
// a single compaction can pick from a specific level
class ILevelSizeRatioPolicy : public SizeRatioPolicy {
 public:
  explicit ILevelSizeRatioPolicy(std::vector<double> ratio_per_level, int num_levels, int default_ratios);
  const char* Name() const { return "ILevelSizeRatioPolicy"; }

  int SizeRatio(int level) const override {
    if (level > static_cast<int>(size_ratio_.size()) - 1) {
      return size_ratio_[size_ratio_.size() - 1];
    }
    return size_ratio_[level];
  }

  void UpdateSizeRatio(int level, int ratio) {
    assert(level <= static_cast<int>(size_ratio_.size()));
    if (level == static_cast<int>(size_ratio_.size())) {
      size_ratio_.push_back(ratio);
    } else {
      size_ratio_[level] = ratio;
    }
  }

 private:
  std::vector<double> size_ratio_{};
};

const SizeRatioPolicy* NewILevelSizeRatioPolicy(
    std::vector<double> size_ratio, int num_levels, int default_ratio) {
  return new ILevelSizeRatioPolicy(size_ratio, num_levels, default_ratio);
}

}  // namespace ROCKSDB_NAMESPACE