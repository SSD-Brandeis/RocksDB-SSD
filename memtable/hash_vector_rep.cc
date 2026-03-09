
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
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace {

using Key = const char*;
using MemtableSkipList = SkipList<Key, const MemTableRep::KeyComparator&>;

class HashVectorRep : public MemTableRep {
 public:
  HashVectorRep(const MemTableRep::KeyComparator& compare, Allocator* allocator,
                const SliceTransform* transform, size_t bucket_size);

  void Insert(KeyHandle handle) override;

  bool Contains(const char* key) const override;

  size_t ApproximateMemoryUsage() override;

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~HashVectorRep() override;

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  MemTableRep::Iterator* GetDynamicPrefixIterator(
      Arena* arena = nullptr) override;

  void MarkReadOnly() override;

 private:
  friend class DynamicIterator;

  size_t bucket_size_;
  std::atomic<size_t> num_entries_{0};

  struct Bucket {
    std::shared_ptr<std::vector<const char*>> vec;
    mutable port::RWMutex rwlock;
    Bucket() : vec(std::make_shared<std::vector<const char*>>()) {}
  };

  Bucket* buckets_;
  std::atomic<bool> immutable_;

  const SliceTransform* transform_;
  const MemTableRep::KeyComparator& compare_;

  Slice GetPrefix(const Slice& internal_key) const {
    // FIX 1: Safely handle missing prefix extractors
    Slice user_key = ExtractUserKey(internal_key);
    return transform_ ? transform_->Transform(user_key) : user_key;
  }

  size_t GetHash(const Slice& slice) const {
    return GetSliceRangedNPHash(slice, bucket_size_);
  }

  Bucket& GetBucket(size_t i) const { return buckets_[i]; }

  Bucket& GetBucket(const Slice& slice) const {
    return GetBucket(GetHash(slice));
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
    // FIX 2: C++ destroys members bottom-to-top.
    // Iter must be destroyed before the skiplist, and skiplist before the arena.
    std::unique_ptr<Allocator> allocator_;
    std::unique_ptr<MemtableSkipList> full_list_;
    MemtableSkipList::Iterator iter_;
    std::string tmp_;
  };

  class DynamicIterator : public MemTableRep::Iterator {
   public:
    explicit DynamicIterator(const HashVectorRep& memtable_rep)
        : memtable_rep_(memtable_rep), bucket_vec_(nullptr), cit_() {}

    bool Valid() const override {
      return bucket_vec_ != nullptr && cit_ != bucket_vec_->end();
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
      if (cit_ == bucket_vec_->begin()) {
        cit_ = bucket_vec_->end();
      } else {
        --cit_;
      }
    }

    void Seek(const Slice& k, const char* memtable_key) override {
      auto transformed = memtable_rep_.GetPrefix(k);
      Bucket& bucket = memtable_rep_.GetBucket(transformed);

      {
        ReadLock l(&bucket.rwlock);
        if (memtable_rep_.immutable_.load(std::memory_order_relaxed)) {
          bucket_vec_ = bucket.vec;
        } else {
          bucket_vec_.reset(new std::vector<const char*>(*bucket.vec));
        }
      }

      const char* encoded_key =
          (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, k);
      cit_ = std::lower_bound(
          bucket_vec_->begin(), bucket_vec_->end(), encoded_key,
          [this](const char* a, const char* b) {
            return memtable_rep_.compare_(a, b) < 0;
          });
    }

    void SeekForPrev(const Slice& /*k*/,
                     const char* /*memtable_key*/) override {
      bucket_vec_.reset();
    }

    void SeekToFirst() override { bucket_vec_.reset(); }

    void SeekToLast() override { bucket_vec_.reset(); }

   private:
    const HashVectorRep& memtable_rep_;
    std::shared_ptr<std::vector<const char*>> bucket_vec_;
    std::vector<const char*>::const_iterator cit_;
    std::string tmp_;
  };
};

HashVectorRep::HashVectorRep(const MemTableRep::KeyComparator& compare,
                             Allocator* allocator,
                             const SliceTransform* transform,
                             size_t bucket_size)
    : MemTableRep(allocator),
      bucket_size_(bucket_size),
      buckets_(new Bucket[bucket_size]),
      immutable_(false),
      transform_(transform),
      compare_(compare) {}

HashVectorRep::~HashVectorRep() { delete[] buckets_; }

void HashVectorRep::Insert(KeyHandle handle) {
  auto* key = static_cast<char*>(handle);
  Slice internal_key = GetLengthPrefixedSlice(key);
  auto transformed = GetPrefix(internal_key);
  Bucket& bucket = GetBucket(transformed);

  WriteLock l(&bucket.rwlock);
  const auto position = std::lower_bound(
      bucket.vec->begin(), bucket.vec->end(), key,
      [this](const char* a, const char* b) { return compare_(a, b) < 0; });
  bucket.vec->insert(position, key);
  
  // FIX 3: Track entries atomically
  num_entries_.fetch_add(1, std::memory_order_relaxed);
}

bool HashVectorRep::Contains(const char* key) const {
  Slice internal_key = GetLengthPrefixedSlice(key);
  auto transformed = GetPrefix(internal_key);
  Bucket& bucket = GetBucket(transformed);

  ReadLock l(&bucket.rwlock);
  auto it = std::lower_bound(
      bucket.vec->begin(), bucket.vec->end(), key,
      [this](const char* a, const char* b) { return compare_(a, b) < 0; });
  return it != bucket.vec->end() && compare_(*it, key) == 0;
}

size_t HashVectorRep::ApproximateMemoryUsage()  { return 0; }

void HashVectorRep::Get(const LookupKey& k, void* callback_args,
                        bool (*callback_func)(void* arg, const char* entry)) {
  // FIX 1: Safely handle missing prefix extractors
  auto transformed = transform_ ? transform_->Transform(k.user_key()) : k.user_key();
  Bucket& bucket = GetBucket(transformed);

  ReadLock l(&bucket.rwlock);
  auto it = std::lower_bound(
      bucket.vec->begin(), bucket.vec->end(), k.memtable_key().data(),
      [this](const char* a, const char* b) { return compare_(a, b) < 0; });

  for (; it != bucket.vec->end(); ++it) {
    if (!callback_func(callback_args, *it)) {
      break;
    }
  }
}

MemTableRep::Iterator* HashVectorRep::GetIterator(Arena* alloc_arena) {
  Arena* new_arena = new Arena(allocator_->BlockSize());
  auto list = new MemtableSkipList(compare_, new_arena);

  for (size_t i = 0; i < bucket_size_; ++i) {
    ReadLock l(&buckets_[i].rwlock);
    for (const char* key : *buckets_[i].vec) {
      list->Insert(key);
    }
  }

  if (alloc_arena == nullptr) {
    return new FullListIterator(list, new_arena);
  } else {
    auto mem = alloc_arena->AllocateAligned(sizeof(FullListIterator));
    return new (mem) FullListIterator(list, new_arena);
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

void HashVectorRep::MarkReadOnly() {
  immutable_.store(true, std::memory_order_relaxed);
}

struct HashVectorRepOptions {
  static const char* kName() { return "HashVectorRepFactoryOptions"; }
  size_t bucket_count;
};

static std::unordered_map<std::string, OptionTypeInfo> hash_vector_info = {
    {"bucket_count",
     {offsetof(struct HashVectorRepOptions, bucket_count), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

class HashVectorRepFactory : public MemTableRepFactory {
 public:
  explicit HashVectorRepFactory(size_t bucket_count) {
    options_.bucket_count = bucket_count;
    RegisterOptions(&options_, &hash_vector_info);
  }

  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* /*logger*/) override {
    return new HashVectorRep(compare, allocator, transform,
                             options_.bucket_count);
  }

  static const char* kClassName() { return "HashVectorRepFactory"; }
  static const char* kNickName() { return "hash_vector"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

 private:
  HashVectorRepOptions options_;
};

}  // namespace

MemTableRepFactory* NewHashVectorRepFactory(size_t bucket_count) {
  return new HashVectorRepFactory(bucket_count);
}

}  // namespace ROCKSDB_NAMESPACE