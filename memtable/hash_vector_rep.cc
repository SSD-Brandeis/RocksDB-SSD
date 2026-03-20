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

  using Bucket = std::vector<const char*>;

  size_t bucket_size_;
  std::atomic<size_t> num_entries_{0};

  std::atomic<size_t> num_initialized_buckets_{0};
  std::atomic<size_t> vector_capacity_bytes_{0};

  std::atomic<Bucket*>* buckets_;
  std::atomic<bool> immutable_;

  const SliceTransform* transform_;
  const MemTableRep::KeyComparator& compare_;

  Slice GetPrefix(const Slice& internal_key) const {
    Slice user_key = ExtractUserKey(internal_key);
    return transform_ ? transform_->Transform(user_key) : user_key;
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
      
      // header
      num_initialized_buckets_.fetch_add(1, std::memory_order_relaxed);
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
      
      cit_ = std::lower_bound(
          bucket_->begin(), bucket_->end(), encoded_key,
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
                             size_t bucket_size)
    : MemTableRep(allocator),
      bucket_size_(bucket_size),
      immutable_(false),
      transform_(transform),
      compare_(compare) {
  auto mem = allocator->AllocateAligned(sizeof(std::atomic<Bucket*>) * bucket_size);
  buckets_ = reinterpret_cast<std::atomic<Bucket*>*>(mem);
  
  for (size_t i = 0; i < bucket_size_; ++i) {
    new (&buckets_[i]) std::atomic<Bucket*>(nullptr);
  }
}

HashVectorRep::~HashVectorRep() = default;

void HashVectorRep::Insert(KeyHandle handle) {
  auto* key = static_cast<char*>(handle);
  Slice internal_key = GetLengthPrefixedSlice(key);
  auto transformed = GetPrefix(internal_key);
  Bucket* bucket = GetInitializedBucket(transformed);


  size_t old_capacity = bucket->capacity();

  const auto position = std::lower_bound(
      bucket->begin(), bucket->end(), key,
      [this](const char* a, const char* b) { return compare_(a, b) < 0; });
  bucket->insert(position, key);
  

  size_t new_capacity = bucket->capacity();
  if (new_capacity > old_capacity) {
    size_t capacity_diff = (new_capacity - old_capacity) * sizeof(const char*);
    vector_capacity_bytes_.fetch_add(capacity_diff, std::memory_order_relaxed);
  }
  
  num_entries_.fetch_add(1, std::memory_order_relaxed);
}

bool HashVectorRep::Contains(const char* key) const {
  Slice internal_key = GetLengthPrefixedSlice(key);
  auto transformed = GetPrefix(internal_key);
  Bucket* bucket = GetBucket(transformed);
  
  if (bucket == nullptr) {
    return false;
  }

  auto it = std::lower_bound(
      bucket->begin(), bucket->end(), key,
      [this](const char* a, const char* b) { return compare_(a, b) < 0; });
  return it != bucket->end() && compare_(*it, key) == 0;
}

size_t HashVectorRep::ApproximateMemoryUsage() { 
  size_t pointer_array_memory = bucket_size_ * sizeof(std::atomic<Bucket*>);
  
  size_t bucket_headers_memory = num_initialized_buckets_.load(std::memory_order_relaxed) * sizeof(Bucket);
  
  size_t vector_buffers_memory = vector_capacity_bytes_.load(std::memory_order_relaxed);
  
  return pointer_array_memory + bucket_headers_memory + vector_buffers_memory;
}

void HashVectorRep::Get(const LookupKey& k, void* callback_args,
                        bool (*callback_func)(void* arg, const char* entry)) {
  auto transformed = transform_ ? transform_->Transform(k.user_key()) : k.user_key();
  Bucket* bucket = GetBucket(transformed);
  
  if (bucket == nullptr) {
    return;
  }

  auto it = std::lower_bound(
      bucket->begin(), bucket->end(), k.memtable_key().data(),
      [this](const char* a, const char* b) { return compare_(a, b) < 0; });

  for (; it != bucket->end(); ++it) {
    if (!callback_func(callback_args, *it)) {
      break;
    }
  }
}

MemTableRep::Iterator* HashVectorRep::GetIterator(Arena* alloc_arena) {
  Arena* new_arena = new Arena(allocator_->BlockSize());
  auto list = new MemtableSkipList(compare_, new_arena);

  for (size_t i = 0; i < bucket_size_; ++i) {
    Bucket* bucket = GetBucket(i);
    if (bucket != nullptr) {
      for (const char* key : *bucket) {
        list->Insert(key);
      }
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