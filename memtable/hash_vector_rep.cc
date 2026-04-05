#include <algorithm>
#include <atomic>
#include <memory>
#include <vector>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/skiplist.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/options_type.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {
namespace {

using Key = const char*;
using MemtableSkipList = SkipList<Key, const MemTableRep::KeyComparator&>;

class HashVectorRep : public MemTableRep {
 public:
  HashVectorRep(const MemTableRep::KeyComparator& compare, Allocator* allocator,
                const SliceTransform* transform, size_t bucket_size,
                Logger* logger, bool if_log_bucket_dist_when_flash);

  void Insert(KeyHandle handle) override;

  bool Contains(const char* key) const override;

  size_t ApproximateMemoryUsage() override;

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~HashVectorRep() override;

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  MemTableRep::Iterator* GetDynamicPrefixIterator(
      Arena* arena = nullptr) override;

 private:
  friend class DynamicIterator;

  std::atomic<size_t> num_entries_{0};
  std::atomic<size_t> initialized_buckets_{0};

  using Bucket = std::vector<const char*>;
  std::atomic<Bucket*>* buckets_;

  size_t bucket_size_;
  const SliceTransform* transform_;
  const MemTableRep::KeyComparator& compare_;
  Logger* logger_;
  bool if_log_bucket_dist_when_flash_;

  Slice GetPrefix(const Slice& internal_key) const {
    return transform_->Transform(ExtractUserKey(internal_key));
  }

  size_t GetHash(const Slice& slice) const {
    return GetSliceRangedNPHash(slice, bucket_size_);
  }

  Bucket* GetBucket(size_t i) const {
    return buckets_[i].load(std::memory_order_acquire);
  }

  Bucket* GetBucket(const Slice& slice) const {
    return GetBucket(GetHash(slice));
  }

  Bucket* GetInitializedBucket(const Slice& transformed) {
    size_t hash = GetHash(transformed);
    Bucket* bucket = GetBucket(hash);
    if (bucket == nullptr) {
      auto addr = allocator_->AllocateAligned(sizeof(Bucket));
      bucket = new (addr) Bucket();
      buckets_[hash].store(bucket, std::memory_order_release);
      initialized_buckets_.fetch_add(1, std::memory_order_relaxed);
    }
    return bucket;
  }

  class FullListIterator : public MemTableRep::Iterator {
   public:
    explicit FullListIterator(MemtableSkipList* list, Allocator* allocator)
        : allocator_(allocator), full_list_(list), iter_(list) {}

    ~FullListIterator() override = default;

    bool Valid() const override { return iter_.Valid(); }
    const char* key() const override { return iter_.key(); }
    void Next() override { iter_.Next(); }
    void Prev() override { iter_.Prev(); }
    void Seek(const Slice& internal_key, const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);
      iter_.Seek(encoded_key);
    }
    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);
      iter_.SeekForPrev(encoded_key);
    }
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void SeekToLast() override { iter_.SeekToLast(); }

   private:
    std::unique_ptr<Allocator> allocator_;
    std::unique_ptr<MemtableSkipList> full_list_;
    MemtableSkipList::Iterator iter_;
    std::string tmp_;
  };

  class DynamicIterator : public MemTableRep::Iterator {
   public:
    explicit DynamicIterator(const HashVectorRep& memtable_rep)
        : memtable_rep_(memtable_rep), bucket_(nullptr), cit_() {}

    bool Valid() const override {
      return bucket_ != nullptr && cit_ != bucket_->end();
    }

    const char* key() const override {
      assert(Valid());
      return *cit_;
    }

    void Next() override {
      assert(Valid());
      ++cit_;
    }

    void Prev() override {
      assert(Valid());
      if (cit_ == bucket_->begin()) {
        cit_ = bucket_->end();
      } else {
        --cit_;
      }
    }

    void Seek(const Slice& k, const char* memtable_key) override {
      auto transformed = memtable_rep_.GetPrefix(k);
      bucket_ = memtable_rep_.GetBucket(transformed);

      if (bucket_ == nullptr) {
        return;
      }

      const char* encoded_key =
          (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, k);

      cit_ = std::lower_bound(bucket_->begin(), bucket_->end(), encoded_key,
                              [this](const char* a, const char* b) {
                                return memtable_rep_.compare_(a, b) < 0;
                              });
    }

    void SeekForPrev(const Slice& /*k*/,
                     const char* /*memtable_key*/) override {
      bucket_ = nullptr;
    }

    void SeekToFirst() override { bucket_ = nullptr; }

    void SeekToLast() override { bucket_ = nullptr; }

   private:
    const HashVectorRep& memtable_rep_;
    const Bucket* bucket_;
    Bucket::const_iterator cit_;
    std::string tmp_;
  };
};

HashVectorRep::HashVectorRep(const MemTableRep::KeyComparator& compare,
                             Allocator* allocator,
                             const SliceTransform* transform,
                             size_t bucket_size, Logger* logger,
                             bool if_log_bucket_dist_when_flash)
    : MemTableRep(allocator),
      bucket_size_(bucket_size),
      transform_(transform),
      compare_(compare),
      logger_(logger),
      if_log_bucket_dist_when_flash_(if_log_bucket_dist_when_flash) {
  auto mem =
      allocator->AllocateAligned(sizeof(std::atomic<void*>) * bucket_size);
  buckets_ = new (mem) std::atomic<Bucket*>[bucket_size];

  for (size_t i = 0; i < bucket_size_; ++i) {
    buckets_[i].store(nullptr, std::memory_order_relaxed);
  }
}

HashVectorRep::~HashVectorRep() = default;

void HashVectorRep::Insert(KeyHandle handle) {
  auto* key = static_cast<char*>(handle);
  auto transformed = transform_->Transform(UserKey(key));
  Bucket* bucket = GetInitializedBucket(transformed);

  auto position = std::lower_bound(
      bucket->begin(), bucket->end(), key,
      [this](const char* a, const char* b) { return compare_(a, b) < 0; });
  bucket->insert(position, key);
  num_entries_.fetch_add(1, std::memory_order_relaxed);
}

bool HashVectorRep::Contains(const char* key) const {
  Slice internal_key = GetLengthPrefixedSlice(key);
  Bucket* bucket = GetBucket(GetPrefix(internal_key));

  if (bucket == nullptr) {
    return false;
  }

  return std::find(bucket->begin(), bucket->end(), key) != bucket->end();
}

size_t HashVectorRep::ApproximateMemoryUsage() {
  return initialized_buckets_.load(std::memory_order_relaxed) * sizeof(Bucket) +
         num_entries_.load(std::memory_order_relaxed) * sizeof(KeyHandle);
}

void HashVectorRep::Get(const LookupKey& k, void* callback_args,
                        bool (*callback_func)(void* arg, const char* entry)) {
  auto transformed = transform_->Transform(k.user_key());
  Bucket* bucket = GetBucket(transformed);

  if (bucket != nullptr) {
    auto it = std::lower_bound(
        bucket->begin(), bucket->end(), k.memtable_key().data(),
        [this](const char* a, const char* b) { return compare_(a, b) < 0; });

    for (; it != bucket->end() && callback_func(callback_args, *it); ++it) {
    }
  }
}

MemTableRep::Iterator* HashVectorRep::GetIterator(Arena* alloc_arena) {
  Arena* arena = new Arena(allocator_->BlockSize());
  auto list = new MemtableSkipList(compare_, arena);
  HistogramImpl keys_per_bucket_hist;

  for (size_t i = 0; i < bucket_size_; ++i) {
    int count = 0;
    Bucket* bucket = GetBucket(i);
    if (bucket != nullptr) {
      for (const char* key : *bucket) {
        list->Insert(key);
        count++;
      }
    }
    if (if_log_bucket_dist_when_flash_) {
      keys_per_bucket_hist.Add(count);
    }
  }

  if (if_log_bucket_dist_when_flash_ && logger_ != nullptr) {
    Info(logger_, "hashVector Entry distribution among buckets: %s",
         keys_per_bucket_hist.ToString().c_str());
  }

  if (alloc_arena == nullptr) {
    return new FullListIterator(list, arena);
  } else {
    auto mem = alloc_arena->AllocateAligned(sizeof(FullListIterator));
    return new (mem) FullListIterator(list, arena);
  }
}

MemTableRep::Iterator* HashVectorRep::GetDynamicPrefixIterator(
    Arena* alloc_arena) {
  if (alloc_arena == nullptr) {
    return new DynamicIterator(*this);
  } else {
    auto mem = alloc_arena->AllocateAligned(sizeof(DynamicIterator));
    return new (mem) DynamicIterator(*this);
  }
}

struct HashVectorRepOptions {
  static const char* kName() { return "HashVectorRepFactoryOptions"; }
  size_t bucket_count;
  bool if_log_bucket_dist_when_flash;
};

static std::unordered_map<std::string, OptionTypeInfo> hash_vector_info = {
    {"bucket_count",
     {offsetof(struct HashVectorRepOptions, bucket_count), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"log_when_flash",
     {offsetof(struct HashVectorRepOptions, if_log_bucket_dist_when_flash),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
};

class HashVectorRepFactory : public MemTableRepFactory {
 public:
  explicit HashVectorRepFactory(size_t bucket_count,
                                bool if_log_bucket_dist_when_flash) {
    options_.bucket_count = bucket_count;
    options_.if_log_bucket_dist_when_flash = if_log_bucket_dist_when_flash;
    RegisterOptions(&options_, &hash_vector_info);
  }

  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override;

  static const char* kClassName() { return "HashVectorRepFactory"; }
  static const char* kNickName() { return "hash_vector"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

 private:
  HashVectorRepOptions options_;
};

}  // namespace

MemTableRep* HashVectorRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* transform, Logger* logger) {
  return new HashVectorRep(compare, allocator, transform, options_.bucket_count,
                           logger, options_.if_log_bucket_dist_when_flash);
}

MemTableRepFactory* NewHashVectorRepFactory(
    size_t bucket_count, bool if_log_bucket_dist_when_flash) {
  return new HashVectorRepFactory(bucket_count, if_log_bucket_dist_when_flash);
}

}  // namespace ROCKSDB_NAMESPACE