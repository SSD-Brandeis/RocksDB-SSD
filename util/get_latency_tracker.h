#pragma once
// Per-Get functional latency breakdown for the VectorRep path.
//
// Usage:
//   GetLatencyTracker tracker;
//   g_get_tracker = &tracker;
//   db->Get(...);
//   g_get_tracker = nullptr;
//   tracker.Print();
//
// Instruments these layers (innermost last):
//   DBImpl::GetImpl
//     MemTable::Get
//       bloom filter check
//       MemTable::GetFromTable
//         VectorRep::Get
//           lock acquire
//           bucket copy
//           Iterator::Seek
//           callback loop

#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <string>

namespace ROCKSDB_NAMESPACE {

struct GetLatencyTracker {
  int64_t dbimpl_getimpl_ns       = 0;
  int64_t memtable_get_ns         = 0;
  int64_t memtable_bloom_ns       = 0;
  int64_t memtable_get_from_table_ns = 0;
  int64_t vectorrep_get_ns        = 0;
  int64_t vectorrep_lock_ns       = 0;
  int64_t vectorrep_bucket_copy_ns = 0;
  int64_t vectorrep_seek_ns       = 0;
  int64_t vectorrep_callback_ns   = 0;

  void Reset() { *this = GetLatencyTracker{}; }

  // Print raw totals for a single Get (stdout).
  void Print() const {
    fprintf(stdout,
      "=== Get Latency Breakdown (ns) ===\n"
      "DBImpl::GetImpl            %8" PRId64 "\n"
      "  MemTable::Get            %8" PRId64 "\n"
      "    bloom filter           %8" PRId64 "\n"
      "    GetFromTable           %8" PRId64 "\n"
      "      VectorRep::Get       %8" PRId64 "\n"
      "        lock acquire       %8" PRId64 "\n"
      "        bucket copy        %8" PRId64 "\n"
      "        Seek               %8" PRId64 "\n"
      "        callback loop      %8" PRId64 "\n",
      dbimpl_getimpl_ns,
      memtable_get_ns,
      memtable_bloom_ns,
      memtable_get_from_table_ns,
      vectorrep_get_ns,
      vectorrep_lock_ns,
      vectorrep_bucket_copy_ns,
      vectorrep_seek_ns,
      vectorrep_callback_ns);
  }

  // One-line CSV header matching CsvRow().
  static const char* CsvHeader() {
    return "op_id"
           ",getimpl_ns"
           ",memtable_get_ns"
           ",bloom_ns"
           ",get_from_table_ns"
           ",vectorrep_get_ns"
           ",lock_ns"
           ",bucket_copy_ns"
           ",seek_ns"
           ",callback_ns";
  }

  // One CSV row for this operation (no trailing newline).
  // Caller owns the buffer; returns a formatted string.
  std::string CsvRow(int64_t op_id) const {
    char buf[256];
    snprintf(buf, sizeof(buf),
             "%" PRId64
             ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64
             ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64,
             op_id,
             dbimpl_getimpl_ns,
             memtable_get_ns,
             memtable_bloom_ns,
             memtable_get_from_table_ns,
             vectorrep_get_ns,
             vectorrep_lock_ns,
             vectorrep_bucket_copy_ns,
             vectorrep_seek_ns,
             vectorrep_callback_ns);
    return buf;
  }
};

// Set this to a tracker before calling DB::Get; clear it afterward.
// Not thread-safe across threads — each thread that wants tracking should
// set its own pointer before the Get call and clear it after.
inline thread_local GetLatencyTracker* g_get_tracker = nullptr;

// ── Internal helpers ──────────────────────────────────────────────────────────

using _Clock = std::chrono::high_resolution_clock;
using _TimePoint = _Clock::time_point;

inline _TimePoint _Now() { return _Clock::now(); }

inline int64_t _ElapsedNs(const _TimePoint& start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             _Now() - start)
      .count();
}

// RAII guard: accumulates elapsed time into tracker->field on destruction.
// Handles multiple return paths cleanly.
// Usage: GET_LATENCY_GUARD(dbimpl_getimpl_ns);
struct _ScopedLatencyGuard {
  int64_t* target_;
  _TimePoint start_;
  _ScopedLatencyGuard(int64_t* t) : target_(t), start_(_Now()) {}
  ~_ScopedLatencyGuard() {
    if (target_) *target_ += _ElapsedNs(start_);
  }
};

// Macro: time a block and accumulate into tracker->field (no-op if no tracker).
// Usage:
//   GET_LATENCY_BEGIN(t0);
//   ... work ...
//   GET_LATENCY_END(t0, vectorrep_seek_ns);
#define GET_LATENCY_BEGIN(var) \
  ::ROCKSDB_NAMESPACE::_TimePoint var = ::ROCKSDB_NAMESPACE::_Now()

#define GET_LATENCY_END(var, field)                               \
  do {                                                            \
    if (::ROCKSDB_NAMESPACE::g_get_tracker) {                     \
      ::ROCKSDB_NAMESPACE::g_get_tracker->field +=                \
          ::ROCKSDB_NAMESPACE::_ElapsedNs(var);                   \
    }                                                             \
  } while (0)

// Macro: RAII guard that times from declaration until end of scope.
// Works with any number of return paths.
// Usage: GET_LATENCY_GUARD(dbimpl_getimpl_ns);
#define GET_LATENCY_GUARD(field)                                         \
  ::ROCKSDB_NAMESPACE::_ScopedLatencyGuard _lat_guard_##field(           \
      ::ROCKSDB_NAMESPACE::g_get_tracker                                  \
          ? &::ROCKSDB_NAMESPACE::g_get_tracker->field                    \
          : nullptr)

}  // namespace ROCKSDB_NAMESPACE
