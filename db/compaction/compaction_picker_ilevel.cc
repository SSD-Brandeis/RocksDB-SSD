#include "db/compaction/compaction_picker_ilevel.h"

#include <string>
#include <utility>
#include <vector>

#include "db/version_edit.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

bool ILevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForForcedBlobGC().empty()) {
    return true;
  }
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  return false;
}

namespace {

enum class CompactToNextLevel {};

// A class to build a i-leveled compaction step-by-step
class ILevelCompactionBuilder {
 public:
  ILevelCompactionBuilder(const std::string& cf_name,
                          VersionStorageInfo* vstorage,
                          CompactionPicker* compaction_picker,
                          LogBuffer* log_buffer,
                          const MutableCFOptions& mutable_cf_options,
                          const ImmutableOptions& ioptions,
                          const MutableDBOptions& mutable_db_options)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        mutable_db_options_(mutable_db_options) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.
  bool SetupOtherL0FilesIfNeeded();

  // Compaction with round-robin compaction priority allows more files to be
  // picked to form a large compaction
  void SetupOtherFilesWithRoundRobinExpansion();
  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // From `start_level_`, pick files to compact to `output_level_`.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one for
  // all compaction priorities except round-robin. For round-robin,
  // multiple consecutive files may be put into inputs->files.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // Return true if a L0 trivial move is picked up.
  bool TryPickL0TrivialMove();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  // When total L0 size is small compared to Lbase, try to pick intra-L0
  // compaction starting from the newest L0 file. This helps to prevent
  // L0->Lbase compaction with large write-amp.
  //
  // Returns true iff an intra-L0 compaction is picked.
  // `start_level_inputs_` and `output_level_` will be updated accordingly if
  // a compaction is picked.
  bool PickSizeBasedIntraL0Compaction();

  // Return true if TrivialMove is extended. `start_index` is the index of
  // the initial file picked, which should already be in `start_level_inputs_`.
  bool TryExtendNonL0TrivialMove(int start_index,
                                 bool only_expand_right = false);

  // Picks a file from level_files to compact.
  // level_files is a vector of (level, file metadata) in ascending order of
  // level. If compact_to_next_level is true, compact the file to the next
  // level, otherwise, compact to the same level as the input file.
  // If skip_last_level is true, skip the last level.
  void PickFileToCompact(
      const autovector<std::pair<int, FileMetaData*>>& level_files,
      CompactToNextLevel compact_to_next_level);

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  bool is_l0_trivial_move_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableOptions& ioptions_;
  const MutableDBOptions& mutable_db_options_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
  int ilevel = mutable_cf_options_.ilevel;
};
}  // namespace

Compaction* ILevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /*existing_snapshots */,
    const SnapshotChecker* /*snapshot_checker*/, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  ILevelCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                  mutable_cf_options, ioptions_,
                                  mutable_db_options);
  return builder.PickCompaction();
}
}  // namespace ROCKSDB_NAMESPACE
