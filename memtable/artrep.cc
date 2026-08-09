//  Custom implementation from SSD-Lab
//
//  Arts is a MemTableRep implementation that uses a ARTSynchronized which forked
//  from https://github.com/flode/ARTSynchronized to intergrate it with RocksDB.
//  It is a trie-based data structure which improves memory footprint by storing
//  keys with similar prefix in a succinct manner.
//

#include "ARTSynchronized/OptimisticLockCoupling/Tree.h"
#include "ARTSynchronized/Key.h"
#include "db/dbformat.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "db/memtable.h"
#include "memory/arena.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/string_util.h"
#include <atomic>
#include <cstdint>
#include <endian.h>

namespace ROCKSDB_NAMESPACE {
namespace {

// Points ART_OLC::N's node allocator at this tree's byte counter for the
// scope's duration, so node memory (allocated outside the arena) is
// visible to ApproximateMemoryUsage().
class ARTNodeAccountingScope {
 public:
  explicit ARTNodeAccountingScope(std::atomic<int64_t>* counter)
      : prev_(ART::MemoryAccounting::currentUsageCounter) {
    ART::MemoryAccounting::currentUsageCounter = counter;
  }
  ~ARTNodeAccountingScope() { ART::MemoryAccounting::currentUsageCounter = prev_; }

  ARTNodeAccountingScope(const ARTNodeAccountingScope&) = delete;
  ARTNodeAccountingScope& operator=(const ARTNodeAccountingScope&) = delete;

 private:
  std::atomic<int64_t>* prev_;
};

class ARTRep : public MemTableRep {
 public:
  explicit ARTRep(const MemTableRep::KeyComparator& cmp, Allocator* allocator);

  virtual ~ARTRep() override;

  virtual KeyHandle Allocate(const size_t len, char** buf) override;

  virtual void Insert(KeyHandle handle) override;

  virtual void InsertWithHint(KeyHandle handle, void** hint) override { Insert(handle); }

  virtual void InsertConcurrently(KeyHandle handle) override;

  virtual void InsertWithHintConcurrently(KeyHandle handle, void** hint) override { InsertConcurrently(handle); }

  virtual bool Contains(const char* key) const override;

  virtual size_t ApproximateMemoryUsage() override;

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg, const char* entry)) override;

  virtual MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  virtual MemTableRep::Iterator* GetDynamicPrefixIterator(
      Arena* arena = nullptr) override {
    return GetIterator(arena);
  }

  // ART specific encoding
  static void EncodeARTKey(const char* memtable_key, Key& art_key);
  static void LoadKeyFromTID(TID tid, Key& key);

 private:
  ART_OLC::Tree tree_;
  const MemTableRep::KeyComparator& cmp_;
  Allocator* const allocator_;
  // Bytes held by this tree's internal nodes (mutable: Contains() is const
  // but can still trigger epoch-deferred node reclamation). Tree's own
  // constructor allocates the root N256 before any scope can be active, so
  // that fixed one-time cost is seeded here rather than captured live.
  mutable std::atomic<int64_t> node_bytes_{sizeof(ART_OLC::N256)};
};

void ARTRep::EncodeARTKey(const char* memtable_key, Key& art_key) {
  uint32_t key_length;
  const char* key_ptr = GetVarint32Ptr(memtable_key, memtable_key + 5, &key_length);
  uint32_t user_key_len = key_length - 8;

  art_key.setKeyLen(key_length);
  memcpy(&art_key[0], key_ptr, user_key_len);

  uint64_t suffix = DecodeFixed64(key_ptr + user_key_len);
  uint64_t be_suffix = htobe64(~suffix);
  memcpy(&art_key[user_key_len], &be_suffix, 8);
}

void ARTRep::LoadKeyFromTID(TID tid, Key& key) {
  const char* memtable_key = reinterpret_cast<const char*>(tid);
  EncodeARTKey(memtable_key, key);
}

ARTRep::ARTRep(const MemTableRep::KeyComparator& cmp, Allocator* allocator)
    : MemTableRep(allocator), tree_(LoadKeyFromTID), cmp_(cmp), allocator_(allocator) {}

ARTRep::~ARTRep() {}

KeyHandle ARTRep::Allocate(const size_t len, char** buf) {
  *buf = allocator_->Allocate(len);
  return static_cast<KeyHandle>(*buf);
}

void ARTRep::Insert(KeyHandle handle) {
  ARTNodeAccountingScope scope(&node_bytes_);
  Key key;
  const char* memtable_key = static_cast<const char*>(handle);
  EncodeARTKey(memtable_key, key);
  auto threadInfo = tree_.getThreadInfo();
  tree_.insert(key, reinterpret_cast<TID>(handle), threadInfo);
}

// ART_OLC::Tree::insert is safe under concurrent callers (optimistic lock
// coupling with per-node write locks), so parallel write-group threads may
// insert directly.
void ARTRep::InsertConcurrently(KeyHandle handle) {
  Insert(handle);
}

bool ARTRep::Contains(const char* key) const {
  ARTNodeAccountingScope scope(&node_bytes_);
  Key art_key;
  EncodeARTKey(key, art_key);
  auto threadInfo = const_cast<ARTRep*>(this)->tree_.getThreadInfo();
  TID res = tree_.lookup(art_key, threadInfo);
  return res != 0;
}

size_t ARTRep::ApproximateMemoryUsage() {
  // Leaf bytes go through the arena and are already counted;
  // this reports only the tree's internal-node overhead.
  int64_t bytes = node_bytes_.load(std::memory_order_relaxed);
  return static_cast<size_t>(bytes > 0 ? bytes : 0);
}

void ARTRep::Get(const LookupKey& k, void* callback_args,
                 bool (*callback_func)(void* arg, const char* entry)) {
  ARTNodeAccountingScope scope(&node_bytes_);
  static constexpr size_t kBatch = 64;
  Key cur_key;
  EncodeARTKey(k.memtable_key().data(), cur_key);
  bool inclusive = true;


  while (true) {
    TID results[kBatch];
    std::size_t resultsFound = 0;
    auto threadInfo = tree_.getThreadInfo();
    tree_.lookupRange(cur_key, results, kBatch, resultsFound, threadInfo);

    const char* last_entry = nullptr;
    for (size_t i = 0; i < resultsFound; ++i) {
      const char* entry = reinterpret_cast<const char*>(results[i]);
      if (!inclusive) {
        Key entry_key;
        EncodeARTKey(entry, entry_key);
        if (entry_key == cur_key) {
          continue;
        }
      }
      last_entry = entry;
      if (!callback_func(callback_args, entry)) {
        return;
      }
    }
    if (resultsFound < kBatch || last_entry == nullptr) {
      return;
    }
    EncodeARTKey(last_entry, cur_key);
    inclusive = false;
  }
}

class ARTIterator : public MemTableRep::Iterator {
 public:
  explicit ARTIterator(const ART_OLC::Tree* tree,
                       std::atomic<int64_t>* node_bytes)
      : tree_(tree), node_bytes_(node_bytes), valid_(false), reverse_(false),
        buffer_idx_(0), buffer_len_(0) {}

  virtual ~ARTIterator() override {}

  virtual bool Valid() const override { return valid_; }

  virtual const char* key() const override {
    assert(valid_);
    return reinterpret_cast<const char*>(buffer_[buffer_idx_]);
  }


  virtual void Next() override {
    assert(valid_);
    if (!reverse_) {
      if (buffer_idx_ + 1 < buffer_len_) {
        buffer_idx_++;
        return;
      }
    } else {
      // Reverse buffer is ordered descending, so the ascending successor of
      // buffer_[i] is buffer_[i-1].
      if (buffer_idx_ > 0) {
        buffer_idx_--;
        return;
      }
    }
    Key current_key;
    ARTRep::EncodeARTKey(CurrentEntry(), current_key);
    FetchNext(current_key, /*inclusive=*/false);
  }

  virtual void Prev() override {
    assert(valid_);
    if (reverse_) {
      if (buffer_idx_ + 1 < buffer_len_) {
        buffer_idx_++;
        return;
      }
    } else {
      // Forward buffer is ordered ascending, so the predecessor of
      // buffer_[i] is buffer_[i-1].
      if (buffer_idx_ > 0) {
        buffer_idx_--;
        return;
      }
    }
    Key current_key;
    ARTRep::EncodeARTKey(CurrentEntry(), current_key);
    FetchPrev(current_key, /*inclusive=*/false);
  }

  virtual void Seek(const Slice& internal_key, const char* memtable_key) override {
    Key start_key;
    EncodeTarget(internal_key, memtable_key, start_key);
    FetchNext(start_key, /*inclusive=*/true);
  }

  virtual void SeekForPrev(const Slice& internal_key, const char* memtable_key) override {
    Key start_key;
    EncodeTarget(internal_key, memtable_key, start_key);
    FetchPrev(start_key, /*inclusive=*/true);
  }

  virtual void SeekToFirst() override {
    Key empty_key;
    empty_key.setKeyLen(0);
    FetchNext(empty_key, /*inclusive=*/true);
  }

  virtual void SeekToLast() override {

    Key max_key;
    max_key.setKeyLen(kMaxSentinelLen);
    memset(&max_key[0], 0xff, kMaxSentinelLen);
    FetchPrev(max_key, /*inclusive=*/true);
  }

 private:
  static constexpr size_t kBufSize = 64;
  static constexpr uint32_t kMaxSentinelLen = 1024;

  const char* CurrentEntry() const {
    return reinterpret_cast<const char*>(buffer_[buffer_idx_]);
  }

  static void EncodeTarget(const Slice& internal_key, const char* memtable_key,
                           Key& art_key) {
    if (memtable_key != nullptr) {
      ARTRep::EncodeARTKey(memtable_key, art_key);
      return;
    }
    art_key.setKeyLen(internal_key.size());
    uint32_t user_key_len = internal_key.size() - 8;
    memcpy(&art_key[0], internal_key.data(), user_key_len);
    uint64_t suffix = DecodeFixed64(internal_key.data() + user_key_len);
    uint64_t be_suffix = htobe64(~suffix);
    memcpy(&art_key[user_key_len], &be_suffix, 8);
  }

  void FetchNext(const Key& start_key, bool inclusive) {
    reverse_ = false;
    Refill(start_key, inclusive, /*backward=*/false);
  }

  void FetchPrev(const Key& start_key, bool inclusive) {
    reverse_ = true;
    Refill(start_key, inclusive, /*backward=*/true);
  }

  void Refill(const Key& start_key, bool inclusive, bool backward) {
    ARTNodeAccountingScope scope(node_bytes_);
    buffer_len_ = 0;
    buffer_idx_ = 0;
    valid_ = false;

    TID temp_buffer[kBufSize];
    std::size_t found = 0;
    auto threadInfo = const_cast<ART_OLC::Tree*>(tree_)->getThreadInfo();
    if (backward) {
      tree_->lookupRangeReverse(start_key, temp_buffer, kBufSize, found,
                                threadInfo);
    } else {
      tree_->lookupRange(start_key, temp_buffer, kBufSize, found, threadInfo);
    }

    for (size_t i = 0; i < found; ++i) {
      const char* entry = reinterpret_cast<const char*>(temp_buffer[i]);
      Key entry_key;
      ARTRep::EncodeARTKey(entry, entry_key);
      if (!inclusive && entry_key == start_key) {
        continue;
      }
      buffer_[buffer_len_++] = temp_buffer[i];
    }
    if (buffer_len_ > 0) {
      valid_ = true;
    }
  }

  const ART_OLC::Tree* tree_;
  std::atomic<int64_t>* node_bytes_;
  bool valid_;

  bool reverse_;
  size_t buffer_idx_;
  size_t buffer_len_;
  TID buffer_[kBufSize];
};

MemTableRep::Iterator* ARTRep::GetIterator(Arena* arena) {
  if (arena != nullptr) {
    void* mem = arena->AllocateAligned(sizeof(ARTIterator));
    return new (mem) ARTIterator(&tree_, &node_bytes_);
  } else {
    return new ARTIterator(&tree_, &node_bytes_);
  }
}

}  // namespace

MemTableRep* ARTRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& cmp, Allocator* allocator,
    const SliceTransform* transform, Logger* logger) {
  return new ARTRep(cmp, allocator);
}

}  // namespace ROCKSDB_NAMESPACE
