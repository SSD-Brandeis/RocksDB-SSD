// tlx_btree_rep.cc
//
// MemTableRep backed by a tlx B+-tree (tlx::btree_set).
//
// Concurrency control: the tlx B+-tree has no internal thread safety, so the
// rep serializes all structural access through a reader-writer lock.
//  - Insert/InsertConcurrently take the write lock (writers are mutually
//    excluded; this also makes RocksDB's parallel write-group path safe).
//  - Contains/Get take the read lock, so lookups from multiple threads
//    proceed concurrently and are correctly excluded against writers.
//  - Iterators never retain a live tlx iterator across calls: a tree insert
//    can split nodes and invalidate iterators, so each iterator operation
//    re-positions by the current key under the read lock (O(log n) per step)
//    and stores only the arena-backed key pointer, which is stable for the
//    lifetime of the memtable.
#include "memtable/tlx_btree_rep.h"

#ifndef ROCKSDB_LITE

#include <atomic>

#include "db/dbformat.h"
#include "rocksdb/memtablerep.h"
#include "db/memtable.h"
#include "memory/arena.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "memtable/wp_rwmutex.h"
#include "util/string_util.h"

namespace rocksdb {

struct KeyComparatorWrapper {
  const MemTableRep::KeyComparator* compare_;

  explicit KeyComparatorWrapper(const MemTableRep::KeyComparator* compare)
      : compare_(compare) {}

  bool operator()(const char* a, const char* b) const {
    return (*compare_)(a, b) < 0;
  }
};

class TLXBTreeRep : public MemTableRep {
 public:
  using Tree = tlx::btree_set<const char*, KeyComparatorWrapper>;

  explicit TLXBTreeRep(const MemTableRep::KeyComparator& cmp,
                       Allocator* allocator)
      : MemTableRep(allocator),
        cmp_(cmp),
        allocator_(allocator),
        tree_(KeyComparatorWrapper(&cmp_)) {}

  virtual ~TLXBTreeRep() override {}

  virtual KeyHandle Allocate(const size_t len, char** buf) override {
    *buf = allocator_->Allocate(len);
    return static_cast<KeyHandle>(*buf);
  }

  virtual void Insert(KeyHandle handle) override {
    const char* key = static_cast<const char*>(handle);
    // Writers first serialize on a spin gate so only one thread at a time
    // contends for the write side of the rwlock. Insert critical sections
    // are ~1 microsecond, so sleeping locks pay futex wakeup latency on
    // every handoff and collapse throughput under contention; spinning
    // keeps the write path at single-writer speed for any thread count
    // (a global-lock B+-tree cannot scale writes further than that).
    while (write_gate_.test_and_set(std::memory_order_acquire)) {
      while (write_gate_.test(std::memory_order_relaxed)) {
        port::AsmVolatilePause();
      }
    }
    {
      WPWriteLock l(&rwlock_);
      tree_.insert(key);
    }
    write_gate_.clear(std::memory_order_release);
  }

  virtual void InsertWithHint(KeyHandle handle, void** hint) override {
    Insert(handle);
  }

  // Called in parallel by multiple write-group threads; inserts are
  // serialized by the write lock while concurrent readers use the read lock.
  virtual void InsertConcurrently(KeyHandle handle) override { Insert(handle); }

  virtual void InsertWithHintConcurrently(KeyHandle handle,
                                          void** hint) override {
    InsertConcurrently(handle);
  }

  virtual bool Contains(const char* key) const override {
    WPReadLock l(&rwlock_);
    return tree_.exists(key);
  }

  virtual size_t ApproximateMemoryUsage() override {
    return 0;
  }

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg, const char* entry)) override {
    const char* target = k.memtable_key().data();
    // The callback chain (SaveValue) does not re-enter the write path, so
    // holding the read lock across the scan is safe and keeps the view of
    // the tree consistent for the duration of the lookup.
    WPReadLock l(&rwlock_);
    auto it = tree_.lower_bound(target);
    for (; it != tree_.end() && callback_func(callback_args, *it); ++it) {
    }
  }

  virtual MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  virtual MemTableRep::Iterator* GetDynamicPrefixIterator(Arena* arena = nullptr) override {
    return GetIterator(arena);
  }

 private:
  friend class TLXBTreeIterator;
  const MemTableRep::KeyComparator& cmp_;
  Allocator* const allocator_;
  Tree tree_;
  mutable WPRWMutex rwlock_;
  mutable std::atomic_flag write_gate_ = ATOMIC_FLAG_INIT;
};

class TLXBTreeIterator : public MemTableRep::Iterator {
 public:
  explicit TLXBTreeIterator(TLXBTreeRep* rep) : rep_(rep), current_(nullptr) {}

  virtual ~TLXBTreeIterator() override {}

  virtual bool Valid() const override { return current_ != nullptr; }

  virtual const char* key() const override {
    assert(Valid());
    return current_;
  }

  virtual void Next() override {
    assert(Valid());
    WPReadLock l(&rep_->rwlock_);
    // Internal keys are unique, so the first key greater than current_ is
    // the successor even if the tree changed since the last call.
    auto it = rep_->tree_.upper_bound(current_);
    current_ = (it != rep_->tree_.end()) ? *it : nullptr;
  }

  virtual void Prev() override {
    assert(Valid());
    WPReadLock l(&rep_->rwlock_);
    // First key >= current_; stepping back yields the predecessor. Keys are
    // never removed, so current_ is still present in the tree.
    auto it = rep_->tree_.lower_bound(current_);
    if (it == rep_->tree_.begin()) {
      current_ = nullptr;
    } else {
      --it;
      current_ = *it;
    }
  }

  virtual void Seek(const Slice& user_key, const char* memtable_key) override {
    const char* encoded_key =
        (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
    WPReadLock l(&rep_->rwlock_);
    auto it = rep_->tree_.lower_bound(encoded_key);
    current_ = (it != rep_->tree_.end()) ? *it : nullptr;
  }

  virtual void SeekForPrev(const Slice& user_key,
                           const char* memtable_key) override {
    const char* encoded_key =
        (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
    WPReadLock l(&rep_->rwlock_);
    auto it = rep_->tree_.upper_bound(encoded_key);
    if (it == rep_->tree_.begin()) {
      current_ = nullptr;
    } else {
      --it;
      current_ = *it;
    }
  }

  virtual void SeekToFirst() override {
    WPReadLock l(&rep_->rwlock_);
    auto it = rep_->tree_.begin();
    current_ = (it != rep_->tree_.end()) ? *it : nullptr;
  }

  virtual void SeekToLast() override {
    WPReadLock l(&rep_->rwlock_);
    auto it = rep_->tree_.end();
    if (it == rep_->tree_.begin()) {
      current_ = nullptr;
    } else {
      --it;
      current_ = *it;
    }
  }

 private:
  TLXBTreeRep* rep_;
  // Arena-backed key pointer of the current entry; stable across tree
  // mutations for the lifetime of the memtable.
  const char* current_;
  std::string tmp_;
};

MemTableRep::Iterator* TLXBTreeRep::GetIterator(Arena* arena) {
  if (arena != nullptr) {
    void* mem = arena->AllocateAligned(sizeof(TLXBTreeIterator));
    return new (mem) TLXBTreeIterator(this);
  } else {
    return new TLXBTreeIterator(this);
  }
}

MemTableRep* TLXBTreeRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& cmp, Allocator* allocator,
    const SliceTransform* transform, Logger* logger) {
  return new TLXBTreeRep(cmp, allocator);
}

MemTableRepFactory* NewTLXBTreeRepFactory() {
  return new TLXBTreeRepFactory();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
