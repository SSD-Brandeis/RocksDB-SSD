#include "db/compaction/compaction_picker_ilevel.h"

#include <string>
#include <utility>
#include <vector>

#include "compaction_ilevel_run_policy.h"
#include "db/ilevel_lsm_policy.h"
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

// FIXME (shubham): currently we have same implementation
enum class CompactToNextLevel {
  kNo,   // compact to the same level as the input file
  kYes,  // compact to the next level except the last level to the same level
  kSkipLastLevel,  // compact to the next level but skip the last level
};

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
        mutable_db_options_(mutable_db_options),
        ilevel_(mutable_cf_options.ilevel),
        level_compaction_granularity_(mutable_cf_options.level_compaction_granularity) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick the initial files to compact to the next level.
  void SetupInitialFiles();

  // // If the initial files are from Li level, pick other Li
  // // files if needed.
  // bool SetupOtherLiFilesIfNeeded();

  // Compaction with round-robin compaction priority allows more files to be
  // picked to form a large compaction
  void SetupOtherFilesWithRoundRobinExpansion();

  // Based on initial files, setup other files need to be compacted
  // in this compaction for level > ilevel + 1, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // From `start_level_`, pick files to compact to `output_level_`.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be:
  //    - all file from start_level_ in case of 0 <= start_level_ <= ilevel
  //    - exactly one file in case of start_level_ > ilevel
  // If level is between [0, ilevel] and there is already a compaction on that
  // level, this function will return false.
  bool PickRunsToCompact();

  // // Return true if a L0 trivial move is picked up.
  // bool TryPickL0TrivialMove();

  // // For L0->L0, picks the longest span of files that aren't currently
  // // undergoing compaction for which work-per-deleted-file decreases. The
  // span
  // // always starts from the newest L0 file.
  // //
  // // Intra-L0 compaction is independent of all other files, so it can be
  // // performed even when L0->base_level compactions are blocked.
  // //
  // // Returns true if `inputs` is populated with a span of files to be
  // compacted;
  // // otherwise, returns false.
  // bool PickIntraL0Compaction();

  // // When total L0 size is small compared to Lbase, try to pick intra-L0
  // // compaction starting from the newest L0 file. This helps to prevent
  // // L0->Lbase compaction with large write-amp.
  // //
  // // Returns true iff an intra-L0 compaction is picked.
  // // `start_level_inputs_` and `output_level_` will be updated accordingly if
  // // a compaction is picked.
  // bool PickSizeBasedIntraL0Compaction();

  // Return true if TrivialMove is extended. `start_index` is the index of
  // the initial file picked, which should already be in `start_level_inputs_`.
  bool TryExtendNoniLTrivialMove(int start_index,
                                 bool only_expand_right = false);

  // // Picks a file from level_files to compact.
  // // level_files is a vector of (level, file metadata) in ascending order of
  // // level. If compact_to_next_level is true, compact the file to the next
  // // level, otherwise, compact to the same level as the input file.
  // // If skip_last_level is true, skip the last level.
  // void PickRunsToCompact(
  //     const autovector<std::pair<int, FileMetaData*>>& level_files,
  //     CompactToNextLevel compact_to_next_level);

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
  bool is_ilevel_trivial_move_ = false;
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
                            int level, int ilevel);

  // static const int kMinFilesForIntraL0Compaction = 4;
  int ilevel_ = 0;

  // Compaction run picker, which decides how many tiers/files a
  // single compaction can pick from a specific level
  std::shared_ptr<const CompactionRunPolicy> level_compaction_granularity_;
};

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t ILevelCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level, int ilevel) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > ilevel) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/main/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size *
                mutable_cf_options.fluidlsm_policy->GetSizeRatio(cur_level));
          } else {
            level_size = static_cast<uint64_t>(
                level_size *
                mutable_cf_options.fluidlsm_policy->GetSizeRatio(cur_level) *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

bool ILevelCompactionBuilder::TryExtendNoniLTrivialMove(
    int start_index, bool only_expand_right) {
  if (start_level_inputs_.size() == 1 &&
      (ioptions_.db_paths.empty() || ioptions_.db_paths.size() == 1) &&
      (mutable_cf_options_.compression_per_level.empty())) {
    // Only file of `index`, and it is likely a trivial move. Try to
    // expand if it is still a trivial move, but not beyond
    // max_compaction_bytes or 4 files, so that we don't create too
    // much compaction pressure for the next level.
    // Ignore if there are more than one DB path, as it would be hard
    // to predict whether it is a trivial move.
    const std::vector<FileMetaData*>& level_files =
        vstorage_->LevelFiles(start_level_);
    const size_t kMaxMultiTrivialMove = 4;
    FileMetaData* initial_file = start_level_inputs_.files[0];
    size_t total_size = initial_file->fd.GetFileSize();
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    // Expand towards right
    for (int i = start_index + 1;
         i < static_cast<int>(level_files.size()) &&
         start_level_inputs_.size() < kMaxMultiTrivialMove;
         i++) {
      FileMetaData* next_file = level_files[i];
      if (next_file->being_compacted) {
        break;
      }
      vstorage_->GetOverlappingInputs(output_level_, &(initial_file->smallest),
                                      &(next_file->largest),
                                      &output_level_inputs.files);
      if (!output_level_inputs.empty()) {
        break;
      }
      if (i < static_cast<int>(level_files.size()) - 1 &&
          compaction_picker_->icmp()
                  ->user_comparator()
                  ->CompareWithoutTimestamp(
                      next_file->largest.user_key(),
                      level_files[i + 1]->smallest.user_key()) == 0) {
        TEST_SYNC_POINT_CALLBACK(
            "ILevelCompactionBuilder::TryExtendNonL0TrivialMove:NoCleanCut",
            nullptr);
        // Not a clean up after adding the next file. Skip.
        break;
      }
      total_size += next_file->fd.GetFileSize();
      if (total_size > mutable_cf_options_.max_compaction_bytes) {
        break;
      }
      start_level_inputs_.files.push_back(next_file);
    }
    // Expand towards left
    if (!only_expand_right) {
      for (int i = start_index - 1;
           i >= 0 && start_level_inputs_.size() < kMaxMultiTrivialMove; i--) {
        FileMetaData* next_file = level_files[i];
        if (next_file->being_compacted) {
          break;
        }
        vstorage_->GetOverlappingInputs(output_level_, &(next_file->smallest),
                                        &(initial_file->largest),
                                        &output_level_inputs.files);
        if (!output_level_inputs.empty()) {
          break;
        }
        if (i > 0 && compaction_picker_->icmp()
                             ->user_comparator()
                             ->CompareWithoutTimestamp(
                                 next_file->smallest.user_key(),
                                 level_files[i - 1]->largest.user_key()) == 0) {
          // Not a clean up after adding the next file. Skip.
          break;
        }
        total_size += next_file->fd.GetFileSize();
        if (total_size > mutable_cf_options_.max_compaction_bytes) {
          break;
        }
        // keep `files` sorted in increasing order by key range
        start_level_inputs_.files.insert(start_level_inputs_.files.begin(),
                                         next_file);
      }
    }
    return start_level_inputs_.size() > 1;
  }
  return false;
}

bool ILevelCompactionBuilder::PickRunsToCompact() {
  // From 0 to ilevel files are overlapping. So we cannot pick more
  // than one concurrent compactions at these levels.
  if (start_level_ <= ilevel_ &&
      !compaction_picker_->ilevel_compactions_in_progress(start_level_)
           ->empty()) {
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }

  start_level_inputs_.clear();
  start_level_inputs_.level = start_level_;

  assert(start_level_ >= 0);

  if (start_level_ <= ilevel_) {
    // if `start_level_` < ilevel, pick all the files.
    const std::vector<SortedRun>& level_runs =
        vstorage_->LevelRuns(start_level_);
    int num_runs = level_compaction_granularity_->PickCompactionCount(start_level_);
    for (int index = static_cast<int>(level_runs.size()) - 1;
         index >= 0 && num_runs > 0; index--) {
      SortedRun run = level_runs[index];
      for (int idx = 0; idx < static_cast<int>(run.size()); idx++) {
        auto* f = run[idx];
        start_level_inputs_.files.push_back(f);
      }
      num_runs--;
    }
    output_level_ = start_level_ + 1;
    // Since this is a tiering implementation, we can return true here
    //  as we don't have to check the overlapping files on the next level
    return true;
  } else {
    const std::vector<FileMetaData*>& level_files =
        vstorage_->LevelFiles(start_level_);
    // pick a single file according to file picking policy
    const std::vector<int>& file_scores =
        vstorage_->FilesByCompactionPri(start_level_);

    unsigned int cmp_idx;
    for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
         cmp_idx < file_scores.size(); cmp_idx++) {
      int index = file_scores[cmp_idx];
      auto* f = level_files[index];

      if (f->being_compacted) {
        if (ioptions_.compaction_pri == kRoundRobin) {
          return false;
        }
        continue;
      }

      start_level_inputs_.files.push_back(f);
      if (start_level_ > ilevel_ &&
          static_cast<int>(start_level_inputs_.size()) <
              level_compaction_granularity_->PickCompactionCount(start_level_) &&
          (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                       &start_level_inputs_) ||
           compaction_picker_->FilesRangeOverlapWithCompaction(
               {start_level_inputs_}, output_level_,
               Compaction::EvaluatePenultimateLevel(
                   vstorage_, mutable_cf_options_, ioptions_, start_level_,
                   output_level_)))) {
        // A locked (pending compaction) input-level file was pulled in due to
        // user-key overlap.
        start_level_inputs_.clear();

        if (ioptions_.compaction_pri == kRoundRobin) {
          return false;
        }
        continue;
      }
      size_t allowed =
          level_compaction_granularity_->PickCompactionCount(start_level_);
      if (start_level_inputs_.size() > allowed) {
        start_level_inputs_.files.resize(allowed);
      }
      // Now that input level is fully expanded, we check whether any output
      // files are locked due to pending compaction.
      //
      // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
      // level files are locked, not just the extra ones pulled in for user-key
      // overlap.
      InternalKey smallest, largest;
      compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
      CompactionInputFiles output_level_inputs;
      output_level_inputs.level = output_level_;
      vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                      &output_level_inputs.files);
      if (output_level_inputs.empty()) {
        if (start_level_ > 0 &&
            TryExtendNoniLTrivialMove(
                index, ioptions_.compaction_pri ==
                           kRoundRobin /* only_expand_right */)) {
          break;
        }
      } else {
        if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                        &output_level_inputs)) {
          start_level_inputs_.clear();
          if (ioptions_.compaction_pri == kRoundRobin) {
            return false;
          }
          continue;
        }
      }

      base_index_ = index;
      break;
    }
    // store where to start the iteration in the next call to PickCompaction
    if (ioptions_.compaction_pri != kRoundRobin) {
      vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);
    }
  }
  return start_level_inputs_.size() > 0;
}

void ILevelCompactionBuilder::SetupInitialFiles() {
  // Find the compactions by:
  //    1) number of files for levels between 0 and ilevel
  //    2) level size for levels > ilevel.
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));

    if (start_level_score_ >= 1) {
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      bool picked_file_to_compact = PickRunsToCompact();
      TEST_SYNC_POINT_CALLBACK("PostPickFileToCompact",
                               &picked_file_to_compact);
      if (picked_file_to_compact) {
        // found the compaction!
        start_level_inputs_.allowed =
            level_compaction_granularity_->PickCompactionCount(start_level_);
        if (start_level_ <= ilevel_) {
          // iLevel score = `num iLevel files` /
          // `level0_file_num_compaction_trigger`
          compaction_reason_ = CompactionReason::kLevelLiFilesNum;
        } else {
          // iLevel+ score = `Level files size` / `MaxBytesForLevel`
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
      }
    } else {
      // Compaction scores are sorted in descending order, no further scores
      // will be >= 1
      break;
    }
  }
  return;
}

void ILevelCompactionBuilder::SetupOtherFilesWithRoundRobinExpansion() {
  // We only expand when the start level is not <= ilevel under round robin
  assert(start_level_ >= ilevel_);

  // For round-robin compaction priority, we have 3 constraints when picking
  // multiple files.
  // Constraint 1: We can only pick consecutive files
  //  -> Constraint 1a: When a file is being compacted (or some input files
  //                    are being compacted after expanding, we cannot
  //                    choose it and have to stop choosing more files
  //  -> Constraint 1b: When we reach the last file (with largest keys), we
  //                    cannot choose more files (the next file will be the
  //                    first one)
  // Constraint 2: We should ensure the total compaction bytes (including the
  //               overlapped files from the next level) is no more than
  //               mutable_cf_options_.max_compaction_bytes
  // Constraint 3: We try our best to pick as many files as possible so that
  //               the post-compaction level size is less than
  //               MaxBytesForLevel(start_level_)
  // Constraint 4: We do not expand if it is possible to apply a trivial move
  // Constraint 5 (TODO): Try to pick minimal files to split into the target
  //               number of subcompactions
  TEST_SYNC_POINT("LevelCompactionPicker::RoundRobin");

  // Only expand the inputs when we have selected a file in start_level_inputs_
  if (start_level_inputs_.size() == 0) {
    return;
  }

  uint64_t start_lvl_bytes_no_compacting = 0;
  uint64_t curr_bytes_to_compact = 0;
  uint64_t start_lvl_max_bytes_to_compact = 0;
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);
  // Constraint 3 (pre-calculate the ideal max bytes to compact)
  for (auto f : level_files) {
    if (!f->being_compacted) {
      start_lvl_bytes_no_compacting += f->fd.GetFileSize();
    }
  }
  if (start_lvl_bytes_no_compacting >
      vstorage_->MaxBytesForLevel(start_level_)) {
    start_lvl_max_bytes_to_compact = start_lvl_bytes_no_compacting -
                                     vstorage_->MaxBytesForLevel(start_level_);
  }

  size_t start_index = vstorage_->FilesByCompactionPri(start_level_)[0];
  InternalKey smallest, largest;
  // Constraint 4 (No need to check again later)
  compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
  CompactionInputFiles output_level_inputs;
  output_level_inputs.level = output_level_;
  vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                  &output_level_inputs.files);
  if (output_level_inputs.empty()) {
    if (TryExtendNoniLTrivialMove((int)start_index,
                                  true /* only_expand_right */)) {
      return;
    }
  }
  // Constraint 3
  if (start_level_inputs_[0]->fd.GetFileSize() >=
      start_lvl_max_bytes_to_compact) {
    return;
  }
  CompactionInputFiles tmp_start_level_inputs;
  tmp_start_level_inputs = start_level_inputs_;
  // TODO (zichen): Future parallel round-robin may also need to update this
  // Constraint 1b (only expand till the end)
  for (size_t i = start_index + 1; i < level_files.size(); i++) {
    auto* f = level_files[i];
    if (f->being_compacted) {
      // Constraint 1a
      return;
    }

    tmp_start_level_inputs.files.push_back(f);
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &tmp_start_level_inputs) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {tmp_start_level_inputs}, output_level_,
            Compaction::EvaluatePenultimateLevel(vstorage_, mutable_cf_options_,
                                                 ioptions_, start_level_,
                                                 output_level_))) {
      // Constraint 1a
      tmp_start_level_inputs.clear();
      return;
    }

    curr_bytes_to_compact = 0;
    for (auto start_lvl_f : tmp_start_level_inputs.files) {
      curr_bytes_to_compact += start_lvl_f->fd.GetFileSize();
    }

    // Check whether any output level files are locked
    compaction_picker_->GetRange(tmp_start_level_inputs, &smallest, &largest);
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      // Constraint 1a
      tmp_start_level_inputs.clear();
      return;
    }

    uint64_t start_lvl_curr_bytes_to_compact = curr_bytes_to_compact;
    for (auto output_lvl_f : output_level_inputs.files) {
      curr_bytes_to_compact += output_lvl_f->fd.GetFileSize();
    }
    if (curr_bytes_to_compact > mutable_cf_options_.max_compaction_bytes) {
      // Constraint 2
      tmp_start_level_inputs.clear();
      return;
    }

    start_level_inputs_.files = tmp_start_level_inputs.files;
    // Constraint 3
    if (start_lvl_curr_bytes_to_compact > start_lvl_max_bytes_to_compact) {
      return;
    }
  }
}

bool ILevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0 to level i, we only
  // compact spans of files that do not interact with any pending compactions,
  // so don't need to consider other levels.
  if (output_level_ > ilevel_) {
    output_level_inputs_.level = output_level_;
    bool round_robin_expanding =
        ioptions_.compaction_pri == kRoundRobin &&
        compaction_reason_ == CompactionReason::kLevelMaxLevelSize;
    if (round_robin_expanding) {
      SetupOtherFilesWithRoundRobinExpansion();
    }
    if (!is_ilevel_trivial_move_ &&
        !compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_,
            round_robin_expanding)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(
            compaction_inputs_, output_level_,
            Compaction::EvaluatePenultimateLevel(vstorage_, mutable_cf_options_,
                                                 ioptions_, start_level_,
                                                 output_level_))) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    if (!is_ilevel_trivial_move_) {
      compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                          output_level_inputs_, &grandparents_);
    }
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

Compaction* ILevelCompactionBuilder::GetCompaction() {
  // TryPickLITrivialMove() does not apply to the case when compacting LI to an
  // empty output level. So LI files is picked in PickRunsToCompact() by
  // compaction score. We may still be able to do trivial move when this file
  // does not overlap with other LIs. This happens when
  // compaction_inputs_[0].size() == 1 since SetupOtherLiFilesIfNeeded() did not
  // pull in more LIs.
  // TODO (steven): need to rewrite the above comment base on our function
  assert(!compaction_inputs_.empty());
  bool ilevel_files_might_overlap =
      start_level_ <= ilevel_ && !is_ilevel_trivial_move_ &&
      (compaction_inputs_.size() > 1 || compaction_inputs_[0].size() > 1);
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(compaction_inputs_), output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_, ilevel_),
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_,
                         vstorage_->base_level()),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_),
      mutable_cf_options_.default_write_temperature,
      /* max_subcompactions */ 0, std::move(grandparents_),
      /* earliest_snapshot */ std::nullopt, /* snapshot_checker */ nullptr,
      is_manual_,
      /* trim_ts */ "", start_level_score_, false /* deletion_compaction */,
      ilevel_files_might_overlap, compaction_reason_);

  // If it's level i or smaller compaction, make sure we don't execute any other
  // related level compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

Compaction* ILevelCompactionBuilder::PickCompaction() {
  // Pick up the first level/file to start compactions.
  //
  // If level between 0 to ilevel needs a compaction then
  // full target level will be compacted like tiering
  //
  // else if level > ilevel needs a compaction then
  // a file(s) will be choosen based on compaction_pri set
  SetupInitialFiles();
  if (start_level_inputs_.empty()) {
    return nullptr;
  }
  assert(start_level_ >= 0 && output_level_ >= 0);

  // Pick files in the output level and expand more files in the start level
  // if needed.
  if (!SetupOtherInputsIfNeeded()) {
    return nullptr;
  }

  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("ILevelCompactionPicker::PickCompaction:Return", c);

  return c;
}

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

CompactionILevelRunPolicy::CompactionILevelRunPolicy(
    std::vector<int> policy_per_level) {
  for (int lvl = 0; lvl < static_cast<int>(policy_per_level.size()); ++lvl) {
    if (lvl == static_cast<int>(level_compaction_granularity_.size())) {
      level_compaction_granularity_.push_back(policy_per_level[lvl]);
    } else {
      level_compaction_granularity_[lvl] = policy_per_level[lvl];
    }
  }
}

ILevelLSMPolicy::ILevelLSMPolicy(std::vector<double> ratio_per_level,
                                 std::vector<double> runs_per_level,
                                 int num_levels, double default_ratio) {
  for (int i = 0; i < num_levels; i++) {
    if (i < static_cast<int>(ratio_per_level.size())) {
      size_ratio_.push_back(ratio_per_level[i]);
    } else {
      size_ratio_.push_back(default_ratio);
    }
  }
  for (int i = 0; i < num_levels; i++) {
    if (i < static_cast<int>(runs_per_level.size())) {
      num_runs_.push_back(runs_per_level[i]);
    } else {
      num_runs_.push_back(1.0);
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE
