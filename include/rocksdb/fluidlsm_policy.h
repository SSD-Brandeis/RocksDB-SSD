
#pragma once

#include <cstdint>
#include <vector>

namespace ROCKSDB_NAMESPACE {

// Size ratio determines number of runs (tierd) or files (leveled)
// a level can hold based on the size of last level * size ratio
class FluidLSMPolicy {
 public:
  virtual ~FluidLSMPolicy() = default;

  // Returns the size ratio from the given level
  virtual double SizeRatio(int level) const = 0;

  // Rrturns the number of runs a levels can hold
  virtual double NumRuns(int level) const = 0;

  virtual const char* Name() const = 0;
};

// Leveled LSM policy follows traditional leveled compaction where
// each level can hold size_ratio times the data of previous level
class LeveledLSMPolicy : public FluidLSMPolicy {
 public:
  LeveledLSMPolicy(int size_ratio) : size_ratio_(size_ratio) {}

  double SizeRatio(int level) const override { return size_ratio_; }
  double NumRuns(int level) const override { return 1; }
  const char* Name() const { return "LeveledLSMPolicy"; }

 private:
  double size_ratio_;
};

// policy per level is configuration through passed vector
const FluidLSMPolicy* NewFluidLSMPolicy(std::vector<double> ratio_per_level,
                                        std::vector<double> runs_per_level,
                                        int num_levels, double default_ratio);

// class CompactionILevelNumFileTriggerPolicy {
//  public:
//   virtual ~CompactionILevelNumFileTriggerPolicy() = default;

//   // Returns the size ratio from the given level
//   virtual int NumFileTrigger(int level) const = 0;

//   virtual const char* Name() const = 0;
// };

// // Always returns 1 for each level
// class FixedNumFileTriggerPolicy : public CompactionILevelNumFileTriggerPolicy
// {
//  public:
//   int NumFileTrigger(int level) const override { return 4; }
//   const char* Name() const { return "FixedNumFileTrigger"; }
// };

// // policy per level is configuration through passed vector
// const CompactionILevelNumFileTriggerPolicy* NewILevelFileCompactTrigger(
//     std::vector<double> ratio_per_level, int ilevel, int default_ratio);
}  // namespace ROCKSDB_NAMESPACE