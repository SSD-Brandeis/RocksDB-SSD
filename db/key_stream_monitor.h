#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "logging/logging.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

class KeyStreamMonitor {
 public:
  static constexpr uint64_t kSampleOneIn = 16;
  static constexpr uint64_t kChunkOps = 10000;
  static constexpr uint64_t kMiB = 1ull << 20;
  static constexpr uint64_t kRecentMiB[4] = {8, 32, 64, 128};

  static KeyStreamMonitor& Instance() {
    static KeyStreamMonitor m;
    return m;
  }

  bool enabled() const { return enabled_; }

  void Attach(const std::shared_ptr<Logger>& log) {
    if (!enabled_) return;
    std::lock_guard<std::mutex> g(mu_);
    log_ = log;
  }

  void Detach() {
    if (!enabled_) return;
    std::lock_guard<std::mutex> g(mu_);
    EmitLocked();
    log_.reset();
  }

  void OnGet(const Slice& key) {
    const uint64_t h = KeyHash(key);
    std::lock_guard<std::mutex> g(mu_);
    if (Sampled(h)) {
      Access(h);
      chunk_.sampled_gets += 1;
      auto it = keys_.find(h);
      if (it == keys_.end()) {
        chunk_.gets_never_written += 1;
      } else if (it->second.deleted) {
        chunk_.gets_deleted += 1;
      } else {
        const uint64_t since = bytes_written_ - it->second.last_write_bytes;
        for (int i = 0; i < 4; ++i)
          if (since <= kRecentMiB[i] * kMiB) chunk_.gets_recent[i] += 1;
      }
    }
    CountLocked();
  }

  void OnSeek(const Slice& key) {
    const uint64_t h = KeyHash(key);
    std::lock_guard<std::mutex> g(mu_);
    if (Sampled(h)) Access(h);
    CountLocked();
  }

  void OnWriteBatch(WriteBatch* batch) {
    Handler h(this);
    batch->Iterate(&h).PermitUncheckedError();
  }

 private:
  struct KeyState {
    uint64_t last_write_bytes;
    bool deleted;
  };
  struct Chunk {
    uint64_t ops = 0, puts = 0, merges = 0, puts_ascending = 0, key_bytes = 0, value_bytes = 0;
    uint64_t sampled_puts = 0, sampled_overwrites = 0, sampled_gets = 0;
    uint64_t gets_never_written = 0, gets_deleted = 0, gets_recent[4] = {0, 0, 0, 0};
  };

  class Handler : public WriteBatch::Handler {
   public:
    explicit Handler(KeyStreamMonitor* m) : m_(m) {}
    Status PutCF(uint32_t, const Slice& key, const Slice& value) override {
      m_->OnPut(key, value.size(), false);
      return Status::OK();
    }
    Status MergeCF(uint32_t, const Slice& key, const Slice& value) override {
      m_->OnPut(key, value.size(), true);
      return Status::OK();
    }
    Status DeleteCF(uint32_t, const Slice& key) override {
      m_->OnDelete(key);
      return Status::OK();
    }
    Status SingleDeleteCF(uint32_t, const Slice& key) override {
      m_->OnDelete(key);
      return Status::OK();
    }
    Status DeleteRangeCF(uint32_t, const Slice& begin, const Slice&) override {
      m_->OnDelete(begin);
      return Status::OK();
    }
    void LogData(const Slice&) override {}

   private:
    KeyStreamMonitor* m_;
  };

  KeyStreamMonitor() {
    const char* v = std::getenv("ROCKSDB_KEY_MONITOR");
    enabled_ = v != nullptr && std::string(v) == "1";
  }

  static uint64_t KeyHash(const Slice& key) { return Hash64(key.data(), key.size()); }

  static bool Sampled(uint64_t h) { return h % kSampleOneIn == 0; }

  void Access(uint64_t h) { access_[h] += 1; }

  void OnPut(const Slice& key, size_t value_bytes, bool merge) {
    const uint64_t h = KeyHash(key);
    std::lock_guard<std::mutex> g(mu_);
    chunk_.puts += 1;
    if (merge) chunk_.merges += 1;
    chunk_.key_bytes += key.size();
    chunk_.value_bytes += value_bytes;
    if (!last_put_key_.empty() && key.compare(Slice(last_put_key_)) > 0) chunk_.puts_ascending += 1;
    last_put_key_.assign(key.data(), key.size());
    bytes_written_ += key.size() + value_bytes;
    if (Sampled(h)) {
      Access(h);
      chunk_.sampled_puts += 1;
      auto it = keys_.find(h);
      if (it != keys_.end() && !it->second.deleted) chunk_.sampled_overwrites += 1;
      keys_[h] = {bytes_written_, false};
    }
    CountLocked();
  }

  void OnDelete(const Slice& key) {
    const uint64_t h = KeyHash(key);
    std::lock_guard<std::mutex> g(mu_);
    bytes_written_ += key.size();
    if (Sampled(h)) {
      Access(h);
      keys_[h] = {bytes_written_, true};
    }
    CountLocked();
  }

  void CountLocked() {
    chunk_.ops += 1;
    if (chunk_.ops >= kChunkOps) EmitLocked();
  }

  void EmitLocked() {
    if (chunk_.ops == 0) return;
    std::vector<uint32_t> counts;
    counts.reserve(access_.size());
    uint64_t total = 0;
    for (const auto& kv : access_) {
      counts.push_back(kv.second);
      total += kv.second;
    }
    std::sort(counts.begin(), counts.end(), std::greater<uint32_t>());
    auto top = [&](double frac) -> uint64_t {
      if (counts.empty()) return 0;
      const size_t n = std::max<size_t>(1, static_cast<size_t>(counts.size() * frac));
      uint64_t s = 0;
      for (size_t i = 0; i < n && i < counts.size(); ++i) s += counts[i];
      return s;
    };
    if (log_) {
      ROCKS_LOG_INFO(
          log_,
          "[key_monitor] ops=%llu one_in=%llu accesses=%llu distinct=%llu top1pct=%llu top10pct=%llu gets=%llu "
          "gets_never_written=%llu gets_deleted=%llu gets_written_within_8mb=%llu gets_written_within_32mb=%llu "
          "gets_written_within_64mb=%llu gets_written_within_128mb=%llu puts=%llu overwrites=%llu "
          "stream_puts=%llu stream_merges=%llu stream_puts_ascending=%llu stream_key_bytes=%llu stream_value_bytes=%llu",
          (unsigned long long)chunk_.ops, (unsigned long long)kSampleOneIn, (unsigned long long)total,
          (unsigned long long)counts.size(), (unsigned long long)top(0.01), (unsigned long long)top(0.10),
          (unsigned long long)chunk_.sampled_gets, (unsigned long long)chunk_.gets_never_written,
          (unsigned long long)chunk_.gets_deleted, (unsigned long long)chunk_.gets_recent[0],
          (unsigned long long)chunk_.gets_recent[1], (unsigned long long)chunk_.gets_recent[2],
          (unsigned long long)chunk_.gets_recent[3], (unsigned long long)chunk_.sampled_puts,
          (unsigned long long)chunk_.sampled_overwrites, (unsigned long long)chunk_.puts, (unsigned long long)chunk_.merges,
          (unsigned long long)chunk_.puts_ascending, (unsigned long long)chunk_.key_bytes,
          (unsigned long long)chunk_.value_bytes);
    }
    chunk_ = Chunk();
    access_.clear();
  }

  bool enabled_ = false;
  std::mutex mu_;
  std::shared_ptr<Logger> log_;
  std::unordered_map<uint64_t, KeyState> keys_;
  std::unordered_map<uint64_t, uint32_t> access_;
  std::string last_put_key_;
  uint64_t bytes_written_ = 0;
  Chunk chunk_;
};

}
