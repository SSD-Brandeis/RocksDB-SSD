#pragma once

#include <algorithm>
#include <atomic>
#include <cmath>
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
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"

namespace ROCKSDB_NAMESPACE {

class PhaseController : public MemtableAdvisor {
 public:
  enum Kind : int { kWrite = 0, kPoint = 1, kRange = 2, kMixed = 3, kKinds = 4 };
  static constexpr uint64_t kWindowsPerBuffer = 8;
  static constexpr uint64_t kMinWindowOps = 2500;
  static constexpr uint64_t kMaxWindowOps = 50000;
  static constexpr int kConfirmWindows = 2;
  static constexpr double kShiftDistance = 0.10;
  static constexpr double kMarginSd = 3.0;
  static constexpr double kRangeShare = 0.02;
  static constexpr double kDominance = 0.70;
  static constexpr int64_t kUnlimitedRate = int64_t{1} << 40;
  static constexpr uint64_t kDefaultDelayedWriteRate = 16 * 1024 * 1024;
  static constexpr int kBadName = -2;

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
    for (int k = 0; k < kKinds; ++k) {
      ColumnFamilyOptions probe;
      s = GetColumnFamilyOptionsFromMap(co, base, sets_[k].options, &probe);
      if (s.ok()) {
        DBOptions probe_db;
        s = GetDBOptionsFromMap(co, dbo, DbOptions(sets_[k]), &probe_db);
      }
      if (!s.ok()) {
        return Status::InvalidArgument(std::string("phase policy [") + Name(k) + "]: " + s.ToString());
      }
      wbs_[k] = probe.write_buffer_size;
    }
    s = GetDBOptionsFromMap(co, dbo, DbOptions(sets_[kMixed]), db);
    if (!s.ok()) return s;
    limiter_.reset();
    for (int k = 0; k < kKinds; ++k) {
      if (sets_[k].rate_bytes_per_sec > 0 && !limiter_) {
        limiter_.reset(NewGenericRateLimiter(Rate(sets_[kMixed]), refill_period_us_, fairness_,
                                             static_cast<RateLimiter::Mode>(mode_), auto_tuned_,
                                             Burst(sets_[kMixed])));
      }
    }
    db->rate_limiter = limiter_;
    ColumnFamilyOptions out;
    s = GetColumnFamilyOptionsFromMap(co, base, sets_[kMixed].options, &out);
    if (!s.ok()) return s;
    current_ = kMixed;
    active_.store(kMixed, std::memory_order_relaxed);
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
    for (auto& c : win_) c.store(0, std::memory_order_relaxed);
    win_ops_.store(0, std::memory_order_relaxed);
    win_bytes_.store(0, std::memory_order_relaxed);
    written_.store(0, std::memory_order_relaxed);
    written_bytes_.store(0, std::memory_order_relaxed);
    window_bytes_.store(wbs_[current_] / kWindowsPerBuffer, std::memory_order_relaxed);
    ops_ = 0;
    ring_n_ = ring_head_ = 0;
    pending_ = 0;
    ROCKS_LOG_INFO(log_, "[phase_controller] policy=%s windows_per_buffer=%llu min_window_ops=%llu max_window_ops=%llu "
                   "confirm_windows=%d shift_distance=%.2f margin_sd=%.1f range_share=%.2f dominance=%.2f start=%s "
                   "memtable_factory=%d write_buffer_size=%llu",
                   path_.c_str(), (unsigned long long)kWindowsPerBuffer, (unsigned long long)kMinWindowOps,
                   (unsigned long long)kMaxWindowOps, kConfirmWindows, kShiftDistance, kMarginSd, kRangeShare,
                   kDominance, Name(current_), sets_[current_].factory, (unsigned long long)wbs_[current_]);
    std::unordered_map<std::string, std::string> global(global_cf_);
    global.insert(global_db_.begin(), global_db_.end());
    ROCKS_LOG_INFO(log_, "[phase_controller] global %s", Join(global).c_str());
    ROCKS_LOG_INFO(log_, "[phase_controller] rate_limiter=%s %s", limiter_ ? "GenericRateLimiter" : "nullptr",
                   Join(limiter_global_).c_str());
    for (int k = 0; k < kKinds; ++k) {
      ROCKS_LOG_INFO(log_, "[phase_controller] set %s memtable_factory=%d %s %s", Name(k), sets_[k].factory,
                     Join(sets_[k].options).c_str(), Calls(sets_[k]).c_str());
    }
    attached_.store(true, std::memory_order_relaxed);
  }

  void Detach() {
    if (!enabled_) return;
    std::lock_guard<std::mutex> g(mu_);
    if (log_ && limiter_) {
      static const char* pri[] = {"IO_LOW", "IO_MID", "IO_HIGH", "IO_USER"};
      for (int p = Env::IO_LOW; p < Env::IO_TOTAL; ++p) {
        ROCKS_LOG_INFO(log_, "[phase_controller] rate_limiter_total %s requests=%lld bytes=%lld", pri[p],
                       (long long)limiter_->GetTotalRequests(static_cast<Env::IOPriority>(p)),
                       (long long)limiter_->GetTotalBytesThrough(static_cast<Env::IOPriority>(p)));
      }
    }
    attached_.store(false, std::memory_order_relaxed);
    db_ = nullptr;
    log_.reset();
  }

  void OnWrites(uint64_t n, uint64_t bytes) { Count(kWrite, n, bytes); }
  void OnGets(uint64_t n) { Count(kPoint, n, 0); }
  void OnSeek() { Count(kRange, 1, 0); }

  int SelectMemtableType(bool) const override { return type_.load(std::memory_order_relaxed); }

  const WriteOptions& ForWrite(const WriteOptions& in, WriteOptions* scratch) const {
    if (!attached_.load(std::memory_order_relaxed)) return in;
    const KnobSet& s = sets_[active_.load(std::memory_order_relaxed)];
    const bool low_pri = s.low_pri >= 0 && (s.low_pri == 1) != in.low_pri;
    const bool priority = s.write_priority >= 0 && !in.disableWAL && s.write_priority != in.rate_limiter_priority;
    if (!low_pri && !priority) return in;
    *scratch = in;
    if (low_pri) scratch->low_pri = s.low_pri == 1;
    if (priority) scratch->rate_limiter_priority = static_cast<Env::IOPriority>(s.write_priority);
    return *scratch;
  }

  void ForRead(ReadOptions* r) const {
    if (!attached_.load(std::memory_order_relaxed)) return;
    const KnobSet& s = sets_[active_.load(std::memory_order_relaxed)];
    if (s.read_priority >= 0) r->rate_limiter_priority = static_cast<Env::IOPriority>(s.read_priority);
  }

 private:
  struct KnobSet {
    int factory = 1;
    std::unordered_map<std::string, std::string> options;
    std::unordered_map<std::string, std::string> db_options;
    std::unordered_map<std::string, std::string> calls;
    int low_pri = -1;
    int write_priority = -1;
    int read_priority = -1;
    int64_t rate_bytes_per_sec = -1;
    int64_t single_burst_bytes = -1;
  };

  static int64_t Rate(const KnobSet& s) { return s.rate_bytes_per_sec > 0 ? s.rate_bytes_per_sec : kUnlimitedRate; }
  static int64_t Burst(const KnobSet& s) { return s.single_burst_bytes > 0 ? s.single_burst_bytes : 0; }

  static std::unordered_map<std::string, std::string> DbOptions(const KnobSet& s) {
    std::unordered_map<std::string, std::string> out(s.db_options);
    auto it = out.find("delayed_write_rate");
    if (it != out.end() && std::strtoull(it->second.c_str(), nullptr, 10) == 0) {
      it->second = std::to_string(s.rate_bytes_per_sec > 0 ? static_cast<uint64_t>(s.rate_bytes_per_sec)
                                                            : kDefaultDelayedWriteRate);
    }
    return out;
  }

  static std::string Calls(const KnobSet& s) {
    std::unordered_map<std::string, std::string> all(s.calls);
    all.insert(s.db_options.begin(), s.db_options.end());
    return Join(all);
  }

  static int Priority(const std::string& v) {
    if (v == "IO_LOW") return Env::IO_LOW;
    if (v == "IO_MID") return Env::IO_MID;
    if (v == "IO_HIGH") return Env::IO_HIGH;
    if (v == "IO_USER") return Env::IO_USER;
    if (v == "IO_TOTAL") return Env::IO_TOTAL;
    return kBadName;
  }

  static int ModeOf(const std::string& v) {
    if (v == "kReadsOnly") return static_cast<int>(RateLimiter::Mode::kReadsOnly);
    if (v == "kWritesOnly") return static_cast<int>(RateLimiter::Mode::kWritesOnly);
    if (v == "kAllIo") return static_cast<int>(RateLimiter::Mode::kAllIo);
    return kBadName;
  }

  static bool Phase(KnobSet* s, const std::string& key, const std::string& value) {
    if (key == "delayed_write_rate") s->db_options[key] = value;
    else if (key == "low_pri") s->low_pri = (value == "true" || value == "1") ? 1 : 0;
    else if (key == "WriteOptions.rate_limiter_priority") s->write_priority = Priority(value);
    else if (key == "ReadOptions.rate_limiter_priority") s->read_priority = Priority(value);
    else if (key == "NewGenericRateLimiter.rate_bytes_per_sec") s->rate_bytes_per_sec = std::strtoll(value.c_str(), nullptr, 10);
    else if (key == "NewGenericRateLimiter.single_burst_bytes") s->single_burst_bytes = std::strtoll(value.c_str(), nullptr, 10);
    else return false;
    if (key != "delayed_write_rate") s->calls[key] = value;
    return true;
  }

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
    else if (key == "NewGenericRateLimiter.refill_period_us") refill_period_us_ = std::strtoll(value.c_str(), nullptr, 10);
    else if (key == "NewGenericRateLimiter.fairness") fairness_ = std::atoi(value.c_str());
    else if (key == "NewGenericRateLimiter.mode") mode_ = ModeOf(value);
    else if (key == "NewGenericRateLimiter.auto_tuned") auto_tuned_ = value == "true" || value == "1";
    else if (key == "max_total_wal_size") global_db_[key] = value;
    else global_cf_[key] = value;
    if (key == "NewGenericRateLimiter.refill_period_us" || key == "NewGenericRateLimiter.fairness" || key == "NewGenericRateLimiter.mode" || key == "NewGenericRateLimiter.auto_tuned") {
      limiter_global_[key] = value;
    }
    return Status::OK();
  }

  Status Check() const {
    if (refill_period_us_ <= 0) return Status::InvalidArgument("phase policy: NewGenericRateLimiter.refill_period_us must be > 0");
    if (fairness_ <= 0) return Status::InvalidArgument("phase policy: NewGenericRateLimiter.fairness must be > 0");
    if (mode_ == kBadName)
      return Status::InvalidArgument("phase policy: NewGenericRateLimiter.mode must be kReadsOnly, kWritesOnly or kAllIo");
    for (int k = 0; k < kKinds; ++k) {
      const KnobSet& s = sets_[k];
      if (s.write_priority == kBadName ||
          (s.write_priority >= 0 && s.write_priority != Env::IO_USER && s.write_priority != Env::IO_TOTAL))
        return Status::InvalidArgument(std::string("phase policy [") + Name(k) +
                                       "]: WriteOptions.rate_limiter_priority must be IO_USER or IO_TOTAL");
      if (s.read_priority == kBadName)
        return Status::InvalidArgument(std::string("phase policy [") + Name(k) +
                                       "]: ReadOptions.rate_limiter_priority must be IO_LOW, IO_MID, IO_HIGH, IO_USER or IO_TOTAL");
      if (s.single_burst_bytes < -1 || s.rate_bytes_per_sec < -1)
        return Status::InvalidArgument(std::string("phase policy [") + Name(k) + "]: rate limiter values must be >= 0");
    }
    return Status::OK();
  }

  Status Load() {
    std::ifstream in(path_);
    if (!in) return Status::InvalidArgument("phase policy: cannot read " + path_);
    for (auto& s : sets_) s = KnobSet();
    cfg_ = DynamicMemtableConfig();
    refill_period_us_ = 100 * 1000;
    fairness_ = 10;
    mode_ = static_cast<int>(RateLimiter::Mode::kWritesOnly);
    auto_tuned_ = false;
    limiter_global_.clear();
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
      } else if (!Phase(&sets_[section], key, value)) {
        sets_[section].options[key] = value;
      }
    }
    for (int k = 0; k < kKinds; ++k)
      if (!seen[k]) return Status::InvalidArgument(std::string("phase policy: missing section ") + Name(k));
    return Check();
  }

  int Classify(const uint64_t* c, int current) const {
    const uint64_t n = c[kWrite] + c[kPoint] + c[kRange];
    if (n == 0) return current;
    auto over = [&](int k, double t) {
      const double m = kMarginSd * std::sqrt(t * (1.0 - t) / static_cast<double>(n));
      return static_cast<double>(c[k]) / static_cast<double>(n) >= t + (k == current ? -m : m);
    };
    if (over(kRange, kRangeShare)) return kRange;
    if (over(kWrite, kDominance)) return kWrite;
    if (over(kPoint, kDominance)) return kPoint;
    return kMixed;
  }

  static double Distance(const uint64_t* a, const uint64_t* b) {
    const double na = static_cast<double>(a[kWrite] + a[kPoint] + a[kRange]);
    const double nb = static_cast<double>(b[kWrite] + b[kPoint] + b[kRange]);
    if (na == 0 || nb == 0) return 0.0;
    double d = 0.0;
    for (int k = kWrite; k <= kRange; ++k) d += std::fabs(a[k] / na - b[k] / nb);
    return d / 2.0;
  }

  uint64_t Limit() const {
    const uint64_t n = written_.load(std::memory_order_relaxed);
    const uint64_t b = written_bytes_.load(std::memory_order_relaxed);
    if (n == 0 || b == 0) return kMaxWindowOps;
    const uint64_t entries = window_bytes_.load(std::memory_order_relaxed) * n / b;
    return std::min(kMaxWindowOps, std::max(kMinWindowOps, entries));
  }

  void Push(const uint64_t* c) {
    for (int k = kWrite; k <= kRange; ++k) ring_[ring_head_][k] = c[k];
    ring_head_ = (ring_head_ + 1) % kWindowsPerBuffer;
    if (ring_n_ < static_cast<int>(kWindowsPerBuffer)) ++ring_n_;
  }

  void Reference(uint64_t* out) const {
    out[kWrite] = out[kPoint] = out[kRange] = 0;
    for (int i = 0; i < ring_n_; ++i)
      for (int k = kWrite; k <= kRange; ++k) out[k] += ring_[i][k];
  }

  void Count(int kind, uint64_t n, uint64_t bytes) {
    if (!attached_.load(std::memory_order_relaxed)) return;
    win_[kind].fetch_add(n, std::memory_order_relaxed);
    if (bytes) {
      win_bytes_.fetch_add(bytes, std::memory_order_relaxed);
      written_.fetch_add(n, std::memory_order_relaxed);
      written_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    }
    if (win_ops_.fetch_add(n, std::memory_order_relaxed) + n >= Limit()) Decide();
  }

  void Decide() {
    DB* db = nullptr;
    int next = -1, prev = -1;
    uint64_t c[kKinds] = {0, 0, 0, 0}, at = 0;
    {
      std::lock_guard<std::mutex> g(mu_);
      const uint64_t limit = Limit();
      if (db_ == nullptr || win_ops_.load(std::memory_order_relaxed) < limit) return;
      const uint64_t done = win_ops_.exchange(0, std::memory_order_relaxed);
      const uint64_t bytes = win_bytes_.exchange(0, std::memory_order_relaxed);
      for (int k = kWrite; k <= kRange; ++k) c[k] = win_[k].exchange(0, std::memory_order_relaxed);
      ops_ += done;
      uint64_t ref[kKinds] = {0, 0, 0, 0};
      Reference(ref);
      const int ref_windows = ring_n_;
      const int seen = Classify(c, current_);
      const double dist = ref_windows ? Distance(c, ref) : 0.0;
      int t = current_;
      const char* decision;
      if (seen != current_ || dist >= kShiftDistance) {
        if (++pending_ >= kConfirmWindows) {
          ring_n_ = ring_head_ = 0;
          Push(c);
          pending_ = 0;
          t = seen;
          decision = "shift";
        } else {
          decision = "differs";
        }
      } else {
        decision = pending_ ? "back" : "same";
        pending_ = 0;
        Push(c);
        uint64_t all[kKinds] = {0, 0, 0, 0};
        Reference(all);
        const int r = Classify(all, current_);
        if (r != current_) {
          t = r;
          decision = "drift";
        }
      }
      if (log_) {
        ROCKS_LOG_INFO(log_, "[phase_controller] window end_ops=%llu ops=%llu limit=%llu write_bytes=%llu writes=%llu "
                       "gets=%llu seeks=%llu ref_windows=%d ref_writes=%llu ref_gets=%llu ref_seeks=%llu distance=%.3f seen=%s "
                       "decision=%s phase=%s",
                       (unsigned long long)ops_, (unsigned long long)done, (unsigned long long)limit,
                       (unsigned long long)bytes,
                       (unsigned long long)c[kWrite], (unsigned long long)c[kPoint], (unsigned long long)c[kRange],
                       ref_windows, (unsigned long long)ref[kWrite], (unsigned long long)ref[kPoint],
                       (unsigned long long)ref[kRange], dist, Name(seen), decision, Name(t));
      }
      if (t == current_) return;
      prev = current_;
      current_ = t;
      type_.store(sets_[t].factory, std::memory_order_relaxed);
      active_.store(t, std::memory_order_relaxed);
      window_bytes_.store(wbs_[t] / kWindowsPerBuffer, std::memory_order_relaxed);
      db = db_;
      next = t;
      at = ops_;
    }
    const KnobSet& a = sets_[prev];
    const KnobSet& b = sets_[next];
    Status s = (b.options.empty() || b.options == a.options)
                   ? Status::OK()
                   : db->SetOptions(db->DefaultColumnFamily(), b.options);
    const auto da = DbOptions(a), dbb = DbOptions(b);
    Status sd = (dbb.empty() || dbb == da) ? Status::OK() : db->SetDBOptions(dbb);
    Status sr;
    if (limiter_) {
      if (Rate(a) != Rate(b)) limiter_->SetBytesPerSecond(Rate(b));
      if (Burst(a) != Burst(b)) sr = limiter_->SetSingleBurstBytes(Burst(b));
    }
    std::lock_guard<std::mutex> g(mu_);
    if (log_) {
      ROCKS_LOG_INFO(log_, "[phase_controller] ops=%llu phase=%s window_writes=%llu window_gets=%llu window_seeks=%llu "
                     "memtable_factory=%d set_options=%s set_db_options=%s set_rate_limiter=%s NewGenericRateLimiter.rate_bytes_per_sec=%lld "
                     "NewGenericRateLimiter.single_burst_bytes=%lld",
                     (unsigned long long)at, Name(next), (unsigned long long)c[kWrite],
                     (unsigned long long)c[kPoint], (unsigned long long)c[kRange], b.factory,
                     s.ok() ? "ok" : s.ToString().c_str(), sd.ok() ? "ok" : sd.ToString().c_str(),
                     sr.ok() ? "ok" : sr.ToString().c_str(), limiter_ ? (long long)limiter_->GetBytesPerSecond() : -1LL,
                     limiter_ ? (long long)limiter_->GetSingleBurstBytes() : -1LL);
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
  std::shared_ptr<RateLimiter> limiter_;
  int64_t refill_period_us_ = 100 * 1000;
  int32_t fairness_ = 10;
  int mode_ = 1;
  bool auto_tuned_ = false;
  std::unordered_map<std::string, std::string> limiter_global_;
  std::atomic<int> type_{1};
  std::atomic<int> active_{kMixed};
  std::atomic<bool> attached_{false};
  int current_ = kMixed;
  int pending_ = 0;
  std::atomic<uint64_t> win_[kKinds] = {};
  std::atomic<uint64_t> win_ops_{0};
  std::atomic<uint64_t> win_bytes_{0};
  std::atomic<uint64_t> window_bytes_{0};
  std::atomic<uint64_t> written_{0};
  std::atomic<uint64_t> written_bytes_{0};
  uint64_t wbs_[kKinds] = {64ull << 20, 64ull << 20, 64ull << 20, 64ull << 20};
  uint64_t ring_[kWindowsPerBuffer][kKinds] = {};
  int ring_n_ = 0, ring_head_ = 0;
  uint64_t ops_ = 0;
};

}
