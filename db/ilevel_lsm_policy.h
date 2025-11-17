#pragma once

#include <assert.h>

#include <iostream>
#include <vector>

#include "rocksdb/fluidlsm_policy.h"

namespace ROCKSDB_NAMESPACE {

// Ilevel compaction run policy determines number of runs (tiers/files)
// a single compaction can pick from a specific level
class ILevelLSMPolicy : public FluidLSMPolicy {
 public:
  explicit ILevelLSMPolicy(std::vector<double> ratio_per_level,
                           std::vector<double> runs_per_level, int num_levels,
                           double default_ratios);
  const char* Name() const { return "ILevelLSMPolicy"; }

  double GetSizeRatio(int level) const override {
    if (level > static_cast<int>(size_ratio_.size()) - 1) {
      return size_ratio_[size_ratio_.size() - 1];
    }
    return size_ratio_[level];
  }

  double GetNumRuns(int level) const override {
    if (level > static_cast<int>(num_runs_.size()) - 1) {
      return num_runs_[num_runs_.size() - 1];
    }
    return num_runs_[level];
  }

  void UpdateSizeRatio(int level, double ratio) {
    assert(level <= static_cast<int>(size_ratio_.size()));
    if (level == static_cast<int>(size_ratio_.size())) {
      size_ratio_.push_back(ratio);
    } else {
      size_ratio_[level] = ratio;
    }
  }

  void UpdateNumRuns(int level, double num_runs) {
    assert(level <= static_cast<int>(num_runs_.size()));
    if (level == static_cast<int>(num_runs_.size())) {
      num_runs_.push_back(num_runs);
    } else {
      num_runs_[level] = num_runs;
    }
  }

 private:
  std::vector<double> size_ratio_{};
  std::vector<double> num_runs_{};
  double default_ratio_;
};

const FluidLSMPolicy* NewFluidLSMPolicy(std::vector<double> ratio_per_level,
                                        std::vector<double> runs_per_level,
                                        int num_levels, double default_ratio) {
  return new ILevelLSMPolicy(ratio_per_level, runs_per_level, num_levels,
                             default_ratio);
}

}  // namespace ROCKSDB_NAMESPACE