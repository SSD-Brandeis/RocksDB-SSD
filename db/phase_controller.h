#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>

#include "logging/logging.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

class PhaseController : public MemtableAdvisor {
 public:
  enum Kind : int { kWrite = 0, kPoint = 1, kRange = 2, kMixed = 3, kKinds = 4 };
  static constexpr uint64_t kChunkOps = 10000;
  static constexpr int kConfirmChunks = 2;
  static constexpr uint64_t kMinPhaseOps = 20000;
  static constexpr double kRangeShare = 0.02;
  static constexpr double kDominance = 0.70;

  static PhaseController& Instance() {
    static PhaseController c;
    return c;
  }

  bool enabled() const { return enabled_; }

  static const char* Name(int kind) {
    static const char* names[kKinds] = {"write", "point_lookup", "range_scan", "mixed"};
    return names[kind];
  }

  Status Configure(ColumnFamilyOptions* cf, DBOptions* db) {
    std::lock_guard<std::mutex> g(mu_);
    Status s = Load();
    if (!s.ok()) return s;
    ConfigOptions co;
    co.ignore_unknown_options = false;
    co.input_strings_escaped = false;
    ColumnFamilyOptions base;
    s = GetColumnFamilyOptionsFromMap(co, *cf, global_cf_, &base);
    if (!s.ok()) return Status::InvalidArgument("phase policy [global]: " + s.ToString());
    DBOptions dbo;
    s = GetDBOptionsFromMap(co, *db, global_db_, &dbo);
    if (!s.ok()) return Status::InvalidArgument("phase policy [global]: " + s.ToString());
    *db = dbo;
    for (int k = 0; k < kKinds; ++k) {
      ColumnFamilyOptions probe;
      s = GetColumnFamilyOptionsFromMap(co, base, sets_[k].options, &probe);
      if (!s.ok()) {
        return Status::InvalidArgument(std::string("phase policy [") + Name(k) + "]: " + s.ToString());
      }
    }
    ColumnFamilyOptions out;
    s = GetColumnFamilyOptionsFromMap(co, base, sets_[kMixed].options, &out);
    if (!s.ok()) return s;
    current_ = kMixed;
    type_.store(sets_[kMixed].factory, std::memory_order_relaxed);
    out.memtable_factory.reset(NewDynamicMemTableFactory(this, cfg_));
    *cf = out;
    return Status::OK();
  }

  void Attach(DB* db, const std::shared_ptr<Logger>& log) {
    if (!enabled_) return;
    std::lock_guard<std::mutex> g(mu_);
    db_ = db;
    log_ = log;
    for (auto& c : chunk_) c.store(0, std::memory_order_relaxed);
    chunk_ops_.store(0, std::memory_order_relaxed);
    ops_ = since_change_ = 0;
    candidate_ = -1;
    agree_ = 0;
    ROCKS_LOG_INFO(log_, "[phase_controller] policy=%s chunk_ops=%llu confirm_chunks=%d min_phase_ops=%llu "
                   "range_share=%.2f dominance=%.2f start=%s memtable_factory=%d",
                   path_.c_str(), (unsigned long long)kChunkOps, kConfirmChunks,
                   (unsigned long long)kMinPhaseOps, kRangeShare, kDominance, Name(current_),
                   sets_[current_].factory);
    std::unordered_map<std::string, std::string> global(global_cf_);
    global.insert(global_db_.begin(), global_db_.end());
    ROCKS_LOG_INFO(log_, "[phase_controller] global %s", Join(global).c_str());
    for (int k = 0; k < kKinds; ++k) {
      ROCKS_LOG_INFO(log_, "[phase_controller] set %s memtable_factory=%d %s", Name(k), sets_[k].factory,
                     Join(sets_[k].options).c_str());
    }
    attached_.store(true, std::memory_order_relaxed);
  }

  void Detach() {
    if (!enabled_) return;
    std::lock_guard<std::mutex> g(mu_);
    attached_.store(false, std::memory_order_relaxed);
    db_ = nullptr;
    log_.reset();
  }

  void OnWrites(uint64_t n) { Count(kWrite, n); }
  void OnGets(uint64_t n) { Count(kPoint, n); }
  void OnSeek() { Count(kRange, 1); }

  int SelectMemtableType(bool) const override { return type_.load(std::memory_order_relaxed); }

 private:
  struct KnobSet {
    int factory = 1;
    std::unordered_map<std::string, std::string> options;
  };

  PhaseController() {
    const char* v = std::getenv("ROCKSDB_PHASE_POLICY");
    path_ = v ? v : "";
    enabled_ = !path_.empty();
  }

  static std::string Join(const std::unordered_map<std::string, std::string>& m) {
    std::map<std::string, std::string> sorted(m.begin(), m.end());
    std::string out;
    for (const auto& kv : sorted) out += (out.empty() ? "" : " ") + kv.first + "=" + kv.second;
    return out;
  }

  static std::string Trim(const std::string& s) {
    const size_t a = s.find_first_not_of(" \t\r");
    if (a == std::string::npos) return "";
    return s.substr(a, s.find_last_not_of(" \t\r") - a + 1);
  }

  Status Global(const std::string& key, const std::string& value) {
    const uint64_t v = std::strtoull(value.c_str(), nullptr, 10);
    if (key == "HashSkipListRepFactory.bucket_count") cfg_.hash_skiplist_bucket_count = v;
    else if (key == "HashSkipListRepFactory.skiplist_height") cfg_.skiplist_height = static_cast<int32_t>(v);
    else if (key == "HashSkipListRepFactory.branching_factor") cfg_.skiplist_branch = static_cast<int32_t>(v);
    else if (key == "HashLinkListRepFactory.bucket_count") cfg_.hash_linklist_bucket_count = v;
    else if (key == "HashLinkListRepFactory.threshold") cfg_.linklist_use_skiplist = static_cast<uint32_t>(v);
    else if (key == "HashVectorRepFactory.bucket_count") cfg_.hash_vector_bucket_count = v;
    else if (key == "max_total_wal_size") global_db_[key] = value;
    else global_cf_[key] = value;
    return Status::OK();
  }

  Status Load() {
    std::ifstream in(path_);
    if (!in) return Status::InvalidArgument("phase policy: cannot read " + path_);
    for (auto& s : sets_) s = KnobSet();
    cfg_ = DynamicMemtableConfig();
    global_cf_.clear();
    global_db_.clear();
    bool seen[kKinds] = {false, false, false, false};
    int section = -2;
    std::string line;
    while (std::getline(in, line)) {
      line = Trim(line);
      if (line.empty() || line[0] == '#') continue;
      if (line.front() == '[' && line.back() == ']') {
        const std::string name = line.substr(1, line.size() - 2);
        section = -2;
        if (name == "global") section = -1;
        for (int k = 0; k < kKinds; ++k)
          if (name == Name(k)) section = k;
        if (section == -2) return Status::InvalidArgument("phase policy: unknown section " + name);
        if (section >= 0) seen[section] = true;
        continue;
      }
      const size_t eq = line.find('=');
      if (eq == std::string::npos || section == -2) return Status::InvalidArgument("phase policy: bad line " + line);
      const std::string key = Trim(line.substr(0, eq)), value = Trim(line.substr(eq + 1));
      if (section == -1) {
        Status s = Global(key, value);
        if (!s.ok()) return s;
      } else if (key == "memtable_factory") {
        sets_[section].factory = std::atoi(value.c_str());
      } else {
        sets_[section].options[key] = value;
      }
    }
    for (int k = 0; k < kKinds; ++k)
      if (!seen[k]) return Status::InvalidArgument(std::string("phase policy: missing section ") + Name(k));
    return Status::OK();
  }

  int Classify(const uint64_t* c) const {
    const uint64_t n = c[kWrite] + c[kPoint] + c[kRange];
    if (n == 0) return current_;
    if (c[kRange] >= kRangeShare * n) return kRange;
    if (c[kWrite] >= kDominance * n) return kWrite;
    if (c[kPoint] >= kDominance * n) return kPoint;
    return kMixed;
  }

  void Count(int kind, uint64_t n) {
    if (!attached_.load(std::memory_order_relaxed)) return;
    chunk_[kind].fetch_add(n, std::memory_order_relaxed);
    if (chunk_ops_.fetch_add(n, std::memory_order_relaxed) + n >= kChunkOps) Decide();
  }

  void Decide() {
    DB* db = nullptr;
    int next = -1, prev = -1;
    uint64_t c[kKinds] = {0, 0, 0, 0}, at = 0;
    {
      std::lock_guard<std::mutex> g(mu_);
      if (db_ == nullptr || chunk_ops_.load(std::memory_order_relaxed) < kChunkOps) return;
      const uint64_t done = chunk_ops_.exchange(0, std::memory_order_relaxed);
      for (int k = 0; k < kKinds; ++k) c[k] = chunk_[k].exchange(0, std::memory_order_relaxed);
      ops_ += done;
      since_change_ += done;
      const int t = Classify(c);
      if (t == current_) {
        candidate_ = -1;
        agree_ = 0;
        return;
      }
      agree_ = (t == candidate_) ? agree_ + 1 : 1;
      candidate_ = t;
      if (agree_ < kConfirmChunks || since_change_ < kMinPhaseOps) return;
      prev = current_;
      current_ = t;
      candidate_ = -1;
      agree_ = 0;
      since_change_ = 0;
      type_.store(sets_[t].factory, std::memory_order_relaxed);
      db = db_;
      next = t;
      at = ops_;
    }
    Status s = (sets_[next].options.empty() || sets_[next].options == sets_[prev].options)
                   ? Status::OK()
                   : db->SetOptions(db->DefaultColumnFamily(), sets_[next].options);
    std::lock_guard<std::mutex> g(mu_);
    if (log_) {
      ROCKS_LOG_INFO(log_, "[phase_controller] ops=%llu phase=%s chunk_writes=%llu chunk_gets=%llu chunk_seeks=%llu "
                     "memtable_factory=%d set_options=%s",
                     (unsigned long long)at, Name(next), (unsigned long long)c[kWrite],
                     (unsigned long long)c[kPoint], (unsigned long long)c[kRange], sets_[next].factory,
                     s.ok() ? "ok" : s.ToString().c_str());
    }
  }

  bool enabled_ = false;
  std::string path_;
  std::mutex mu_;
  DB* db_ = nullptr;
  std::shared_ptr<Logger> log_;
  KnobSet sets_[kKinds];
  std::unordered_map<std::string, std::string> global_cf_;
  std::unordered_map<std::string, std::string> global_db_;
  DynamicMemtableConfig cfg_;
  std::atomic<int> type_{1};
  std::atomic<bool> attached_{false};
  int current_ = kMixed;
  int candidate_ = -1;
  int agree_ = 0;
  std::atomic<uint64_t> chunk_[kKinds] = {};
  std::atomic<uint64_t> chunk_ops_{0};
  uint64_t ops_ = 0, since_change_ = 0;
};

}
