#pragma once

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {
// Picking compactions for i-leveled compaction.
class ILevelCompactionPicker : public CompactionPicker {
 public:
  ILevelCompactionPicker(const ImmutableOptions& ioptions,
                         const InternalKeyComparator* icmp,
                        ColumnFamilyData* cfd)
      : CompactionPicker(ioptions, icmp), cfd_(cfd) {}
  Compaction* PickCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& /* existing_snapshots */,
      const SnapshotChecker* /* snapshot_checker */,
      VersionStorageInfo* vstorage, LogBuffer* log_buffer,
      uint64_t max_memtable_id) override;

  bool NeedsCompaction(const VersionStorageInfo* vstorage) const override;

 private:
  // column family data is needed to find immutable memtable
  // in ILevelCompactionPicker to support pure leveling
  ColumnFamilyData* cfd_ = nullptr;
};

}  // namespace ROCKSDB_NAMESPACE
