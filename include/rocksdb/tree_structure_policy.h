
#pragma once

#include <cstdint>
#include <vector>

namespace ROCKSDB_NAMESPACE {

// Size ratio determines number of runs (tierd) or files (leveled)
// a level can hold based on the size of last level * size ratio of 
// this level
//
class SizeRatioPolicy {
 public:
  virtual ~SizeRatioPolicy() = default;

  // Returns the size ratio from the given level
  virtual int SizeRatio(int level) const = 0;

  virtual const char* Name() const = 0;
};

// Always returns 1 for each level
class FixedSizeRatioPolicy : public SizeRatioPolicy {
 public:
  int SizeRatio(int level) const override { return 4; }
  const char* Name() const { return "FixedSizeRatios"; }
};

// policy per level is configuration through passed vector
const SizeRatioPolicy* NewILevelSizeRatioPolicy(
    std::vector<double> ratio_per_level, int num_levels, int default_ratio);

class CompactionILevelNumFileTriggerPolicy {
 public:
  virtual ~CompactionILevelNumFileTriggerPolicy() = default;

  // Returns the size ratio from the given level
  virtual int NumFileTrigger(int level) const = 0;

  virtual const char* Name() const = 0;
};

// Always returns 1 for each level
class FixedNumFileTriggerPolicy : public CompactionILevelNumFileTriggerPolicy {
 public:
  int NumFileTrigger(int level) const override { return 4; }
  const char* Name() const { return "FixedNumFileTrigger"; }
};

// policy per level is configuration through passed vector
const CompactionILevelNumFileTriggerPolicy* NewILevelFileCompactTrigger(
    std::vector<double> ratio_per_level, int ilevel, int default_ratio);
}  // namespace ROCKSDB_NAMESPACE