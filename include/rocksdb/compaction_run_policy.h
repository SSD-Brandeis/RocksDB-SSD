
#pragma once

#include <cstdint>
#include <vector>

namespace ROCKSDB_NAMESPACE {

// Compaction run policy determines the number of runs
// a single compaction can pick from specific level
//
// This assumes that levels are following different layouts
// and it returns a number which tells how many files, in leveled,
// or how many tiers, in tiered level a single compaction can pick
class CompactionRunPolicy {
 public:
  virtual ~CompactionRunPolicy() = default;

  // Returns the number of runs (for tiered) or files (for leveled)
  // to compaction from the given level
  virtual int PickCompactionCount(int level) const = 0;

  virtual const char* Name() const = 0;
};

// Always returns 1 for each level
class FixedCompactionRunPolicy : public CompactionRunPolicy {
 public:
  int PickCompactionCount(int level) const override { return 1; }
  const char* Name() const { return "FixedCompactionRunPolicy"; }
};

// policy per level is configuration through passed vector
const CompactionRunPolicy* NewCompactionILevelRunPolicy(
    std::vector<int> policy_per_level);

}  // namespace ROCKSDB_NAMESPACE