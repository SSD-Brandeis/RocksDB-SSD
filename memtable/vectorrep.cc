//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <algorithm>
#include <memory>
#include <set>
#include <type_traits>
#include <unordered_set>
#include <limits>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/stl_wrappers.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/options_type.h"
#include "util/mutexlock.h"

#include <chrono>
#include <iostream>
// #define TIMER
namespace ROCKSDB_NAMESPACE {
namespace {

class VectorRep : public MemTableRep {
 public:
  VectorRep(const KeyComparator& compare, Allocator* allocator, size_t count);

  // Insert key into the collection. (The caller will pack key and value into a
  // single buffer and pass that in as the parameter to Insert)
  // REQUIRES: nothing that compares equal to key is currently in the
  // collection.
  void Insert(KeyHandle handle) override;

  // Returns true iff an entry that compares equal to key is in the collection.
  bool Contains(const char* key) const override;

  void MarkReadOnly() override;

  size_t ApproximateMemoryUsage() override;

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~VectorRep() override = default;

  class Iterator : public MemTableRep::Iterator {
    class VectorRep* vrep_;
    std::shared_ptr<std::vector<const char*>> bucket_;
    std::vector<const char*>::const_iterator mutable cit_;
    const KeyComparator& compare_;
    std::string tmp_;  // For passing to EncodeKey
    bool mutable sorted_;
    void DoSort() const;

   public:
    explicit Iterator(class VectorRep* vrep,
                      std::shared_ptr<std::vector<const char*>> bucket,
                      const KeyComparator& compare);

    // Initialize an iterator over the specified collection.
    // The returned iterator is not valid.
    // explicit Iterator(const MemTableRep* collection);
    ~Iterator() override = default;

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override;

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() override;

    // Advance to the first entry with a key >= target
    void Seek(const Slice& user_key, const char* memtable_key) override;

    // Advance to the first entry with a key <= target
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override;

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    void SeekToFirst() override;

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    void SeekToLast() override;
  };

  MemTableRep::Iterator* GetIterator(Arena* arena) override;

 protected:
  friend class Iterator;
  using Bucket = std::vector<const char*>;
  std::shared_ptr<Bucket> bucket_;
  mutable port::RWMutex rwlock_;
  bool immutable_;
  bool sorted_;
  const KeyComparator& compare_;
};

// -------------------------------------------------------------------------
// UnsortedVectorRep
// -------------------------------------------------------------------------
class UnsortedVectorRep : public VectorRep {
 public:
  UnsortedVectorRep(const KeyComparator& compare, Allocator* allocator,
                    size_t count)
      : VectorRep(compare, allocator, count) {}

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~UnsortedVectorRep() override = default;

  class Iterator : public MemTableRep::Iterator {
    // class UnsortedVectorRep* vrep_;
    std::shared_ptr<std::vector<const char*>> bucket_;
    std::vector<const char*>::const_iterator mutable cit_;
    const KeyComparator& compare_;
    std::string tmp_;  // For passing to EncodeKey
    // bool mutable sorted_;

   public:
    explicit Iterator(class UnsortedVectorRep* vrep,
                      std::shared_ptr<std::vector<const char*>> bucket,
                      const KeyComparator& compare);

    // Initialize an iterator over the specified collection.
    // The returned iterator is not valid.
    // explicit Iterator(const MemTableRep* collection);
    ~Iterator() override = default;
    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override;
    
    // [MODIFIED] Changed from  increment to logical search
    void Next() override;
    
    // [MODIFIED] Changed from decrement to logical search
    void Prev() override;
    
    // [MODIFIED] Changed from finding first *physical* match to finding *logical* lower bound
    void Seek(const Slice& user_key, const char* memtable_key) override;
    
    // [ADDED] changed from asserted false to implement SeekForPrev 
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override;
    
    // [MODIFIED] Changed to search for logical minimum
    void SeekToFirst() override;
    
    // [MODIFIED] Changed to search for logical maximum
    void SeekToLast() override;
  };
};

void VectorRep::Insert(KeyHandle handle) {
#ifdef TIMER
  auto start = std::chrono::high_resolution_clock::now();
#endif
  auto* key = static_cast<char*>(handle);
  WriteLock l(&rwlock_);
#ifdef TIMER
  auto stop1 = std::chrono::high_resolution_clock::now();
  auto duration1 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop1 - start);
  // std::cout << "Lock: " << duration1.count() << ", " << std::flush;
  std::cout << "Lock: " << duration1.count() << ", ";
#endif
  
  assert(!immutable_);
  bucket_->push_back(key);

#ifdef TIMER
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - stop1);
  // std::cout << "VectorRep: " << duration.count() << ", " << std::flush;
  std::cout << "VectorRep: " << duration.count() << std::endl << std::flush;
#endif
  //ignore lock timing 
// #ifdef TIMER
//   auto stop = std::chrono::high_resolution_clock::now();
//   auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//     // std::cout << "VectorRep: " << duration.count() << ", " << std::flush;
//   std::cout << "VectorRep: " << duration.count() << ", " << std::endl << std::flush;
// #endif

}

// Returns true iff an entry that compares equal to key is in the collection.
bool VectorRep::Contains(const char* key) const {
  ReadLock l(&rwlock_);
  return std::find(bucket_->begin(), bucket_->end(), key) != bucket_->end();
}

void VectorRep::MarkReadOnly() {
  WriteLock l(&rwlock_);
  immutable_ = true;
}

size_t VectorRep::ApproximateMemoryUsage() {
  return sizeof(bucket_) + sizeof(*bucket_) +
         bucket_->size() *
             sizeof(
                 std::remove_reference<decltype(*bucket_)>::type::value_type);
}

VectorRep::VectorRep(const KeyComparator& compare, Allocator* allocator,
                     size_t count)
    : MemTableRep(allocator),
      bucket_(new Bucket()),
      immutable_(false),
      sorted_(false),
      compare_(compare) {
  bucket_.get()->reserve(count);
}

VectorRep::Iterator::Iterator(class VectorRep* vrep,
                              std::shared_ptr<std::vector<const char*>> bucket,
                              const KeyComparator& compare)
    : vrep_(vrep),
      bucket_(bucket),
      cit_(bucket_->end()),
      compare_(compare),
      sorted_(false) {}

void VectorRep::Iterator::DoSort() const {
  if (!sorted_ && vrep_ != nullptr) {
    WriteLock l(&vrep_->rwlock_);
    if (!vrep_->sorted_) {
      std::sort(bucket_->begin(), bucket_->end(),
                stl_wrappers::Compare(compare_));
      cit_ = bucket_->begin();
      vrep_->sorted_ = true;
    }
    sorted_ = true;
  }
  if (!sorted_) {
    std::sort(bucket_->begin(), bucket_->end(),
              stl_wrappers::Compare(compare_));
    cit_ = bucket_->begin();
    sorted_ = true;
  }
  assert(sorted_);
  assert(vrep_ == nullptr || vrep_->sorted_);
}

// Returns true iff the iterator is positioned at a valid node.
bool VectorRep::Iterator::Valid() const {
  DoSort();
  return cit_ != bucket_->end();
}

// Returns the key at the current position.
// REQUIRES: Valid()
const char* VectorRep::Iterator::key() const {
  assert(sorted_);
  return *cit_;
}

// Advances to the next position.
// REQUIRES: Valid()
void VectorRep::Iterator::Next() {
  assert(sorted_);
  if (cit_ == bucket_->end()) {
    return;
  }
  ++cit_;
}

// Advances to the previous position.
// REQUIRES: Valid()
void VectorRep::Iterator::Prev() {
  assert(sorted_);
  if (cit_ == bucket_->begin()) {
    // If you try to go back from the first element, the iterator should be
    // invalidated. So we set it to past-the-end. This means that you can
    // treat the container circularly.
    cit_ = bucket_->end();
  } else {
    --cit_;
  }
}

// Advance to the first entry with a key >= target
void VectorRep::Iterator::Seek(const Slice& user_key,
                               const char* memtable_key) {
// #ifdef GET_TIMER
//       auto start = std::chrono::high_resolution_clock::now();
// #endif // GET_TIMER                                
  DoSort();
// #ifdef GET_TIMER
//       auto stop = std::chrono::high_resolution_clock::now();
//       auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//       // std::cout << __FILE__ << ":" << __LINE__ << " " << __FUNCTION__ << ": " << duration.count() << ", " << std::flush;
//       std::cout << "VectorRep::" << __FUNCTION__ << ": " << duration.count() << ", " << std::flush;
// #endif // GET_TIMER
  // Do binary search to find first value not less than the target
  const char* encoded_key =
      (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
  cit_ = std::equal_range(bucket_->begin(), bucket_->end(), encoded_key,
                          [this](const char* a, const char* b) {
                            return compare_(a, b) < 0;
                          })
             .first;
}

// Advance to the first entry with a key <= target
void VectorRep::Iterator::SeekForPrev(const Slice& /*user_key*/,
                                      const char* /*memtable_key*/) {
  assert(false);
}

// Position at the first entry in collection.
// Final state of iterator is Valid() iff collection is not empty.
void VectorRep::Iterator::SeekToFirst() {
  DoSort();
  cit_ = bucket_->begin();
}

// Position at the last entry in collection.
// Final state of iterator is Valid() iff collection is not empty.
void VectorRep::Iterator::SeekToLast() {
  DoSort();
  cit_ = bucket_->end();
  if (bucket_->size() != 0) {
    --cit_;
  }
}

void VectorRep::Get(const LookupKey& k, void* callback_args,
                    bool (*callback_func)(void* arg, const char* entry)) {
// #ifdef GET_TIMER
//       auto start = std::chrono::high_resolution_clock::now();
// #endif // GET_TIMER
#ifdef GET_TIMER
  // 1. Start Copy Timer
  auto start_copy = std::chrono::high_resolution_clock::now();
#endif
//snpshot start
  rwlock_.ReadLock();
  VectorRep* vector_rep;
  std::shared_ptr<Bucket> bucket;
  if (immutable_) {
    vector_rep = this;
  } else {
    vector_rep = nullptr;
    bucket.reset(new Bucket(*bucket_));  // make a copy
  }
  //snapshot end
  VectorRep::Iterator iter(vector_rep, immutable_ ? bucket_ : bucket, compare_);
  rwlock_.ReadUnlock();

  #ifdef GET_TIMER
  // 2. "Sorted_Copy"
  auto end_copy = std::chrono::high_resolution_clock::now();
  auto copy_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_copy - start_copy);
  std::cout << "Sorted_Copy: " << copy_duration.count() << ", " << std::flush;

  // 3. Start Timer
  auto start_search = std::chrono::high_resolution_clock::now();
#endif
  for (iter.Seek(k.user_key(), k.memtable_key().data());
       iter.Valid() && callback_func(callback_args, iter.key()); iter.Next()) {
  }
// #ifdef GET_TIMER
//       auto stop = std::chrono::high_resolution_clock::now();
//       auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//       std::cout << "VectorRep::" << __FUNCTION__ << ": " << duration.count() << ", " << std::flush;
// #endif // GET_TIMER
#ifdef GET_TIMER
  // 4. End Search Timer & Log "Sorted_Search"
  auto end_search = std::chrono::high_resolution_clock::now();
  auto search_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_search - start_search);
  std::cout << "Sorted_Search: " << search_duration.count() << ", " << std::flush;
#endif
}

MemTableRep::Iterator* VectorRep::GetIterator(Arena* arena) {
  char* mem = nullptr;
  if (arena != nullptr) {
    mem = arena->AllocateAligned(sizeof(Iterator));
  }
  ReadLock l(&rwlock_);
  // Do not sort here. The sorting would be done the first time
  // a Seek is performed on the iterator.
  if (immutable_) {
    if (arena == nullptr) {
      return new Iterator(this, bucket_, compare_);
    } else {
      return new (mem) Iterator(this, bucket_, compare_);
    }
  } else {
    std::shared_ptr<Bucket> tmp;
    tmp.reset(new Bucket(*bucket_));  // make a copy
    if (arena == nullptr) {
      return new Iterator(nullptr, tmp, compare_);
    } else {
      return new (mem) Iterator(nullptr, tmp, compare_);
    }
  }
}

//unsorted vector
UnsortedVectorRep::Iterator::Iterator(
    class UnsortedVectorRep* vrep,
    std::shared_ptr<std::vector<const char*>> bucket,
    const KeyComparator& compare)
    : bucket_(bucket),
      cit_(bucket_->end()),
      compare_(compare)
      {}

// Returns the key at the current position.
// REQUIRES: Valid()
bool UnsortedVectorRep::Iterator::Valid() const {
  return cit_ != bucket_->end();
}

const char* UnsortedVectorRep::Iterator::key() const { return *cit_; }

// [MODIFIED] Next()
// Was: ++cit; moved to the next mem loc. wrong logic for unsorted buffer
// changed to: Performs O(N) scan to find the smallest key strictly greater than current. this logic is correct
void UnsortedVectorRep::Iterator::Next() {
  if (cit_ == bucket_->end()) return;

  const char* current_key = *cit_;
  auto best_candidate = bucket_->end();

  // Linear scan to find logical successor
  for (auto it = bucket_->begin(); it != bucket_->end(); ++it) {
    if (compare_(*it, current_key) > 0) {
      if (best_candidate == bucket_->end() || compare_(*it, *best_candidate) < 0) {
        best_candidate = it;
      }
    }
  }
  cit_ = best_candidate;
}

// [MODIFIED] Prev()
// Was: --cit_; moved to the prev mem loc. wrong logic for unsorted buffer
// changed to: Performs O(N) scan to find largest key strictly smaller than current.
void UnsortedVectorRep::Iterator::Prev() {
  if (cit_ == bucket_->end()) {
    SeekToLast(); 
    return;
  }

  const char* current_key = *cit_;
  auto best_candidate = bucket_->end();

  // Linear scan to find logical predecessor
  for (auto it = bucket_->begin(); it != bucket_->end(); ++it) {
    if (compare_(*it, current_key) < 0) {
      if (best_candidate == bucket_->end() || compare_(*it, *best_candidate) > 0) {
        best_candidate = it;
      }
    }
  }
  cit_ = best_candidate;
}

// [MODIFIED] Seek()
// Was: std::find_if(...) (Stopped at first physical match >= target, only valid if buffer is sorted)
// changedto: Performs O(N) scan to find the smallest lower bound 
void UnsortedVectorRep::Iterator::Seek(const Slice& user_key,
                                       const char* memtable_key) {
  const char* encoded_key =
      (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
  
  auto best_candidate = bucket_->end();

  for (auto it = bucket_->begin(); it != bucket_->end(); ++it) {
    // Check if item >= target
    if (compare_(*it, encoded_key) >= 0) {
      // Optimization: if exact match, we stop
      if (compare_(*it, encoded_key) == 0) {
         cit_ = it;
         return; 
      }
      // Otherwise, track the smallest key that satisfies the condition
      if (best_candidate == bucket_->end() || compare_(*it, *best_candidate) < 0) {
        best_candidate = it;
      }
    }
  }
  cit_ = best_candidate;
}

// [ADDED] SeekForPrev()
// Was: assert(false);
// changed to: Performs O(N) scan to find the largest key <= target.
void UnsortedVectorRep::Iterator::SeekForPrev(const Slice& user_key,
                                              const char* memtable_key) {
  const char* encoded_key =
      (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
      
  auto best_candidate = bucket_->end();

  for (auto it = bucket_->begin(); it != bucket_->end(); ++it) {
    // Check if item <= target
    if (compare_(*it, encoded_key) <= 0) {
       // Track the largest key that satisfies the condition
       if (best_candidate == bucket_->end() || compare_(*it, *best_candidate) > 0) {
         best_candidate = it;
       }
    }
  }
  cit_ = best_candidate;
}

// [MODIFIED] SeekToFirst()
// Was: cit_ = bucket_->begin(); 
// chnaged to: Scans for logical min.
void UnsortedVectorRep::Iterator::SeekToFirst() { 
  auto best_candidate = bucket_->end();
  for (auto it = bucket_->begin(); it != bucket_->end(); ++it) {
    if (best_candidate == bucket_->end() || compare_(*it, *best_candidate) < 0) {
      best_candidate = it;
    }
  }
  cit_ = best_candidate;
}

// [MODIFIED] SeekToLast()
// Was: cit_ = bucket_->end() - 1; 
// Now: Scans for logical max.
void UnsortedVectorRep::Iterator::SeekToLast() {
  auto best_candidate = bucket_->end();
  for (auto it = bucket_->begin(); it != bucket_->end(); ++it) {
    if (best_candidate == bucket_->end() || compare_(*it, *best_candidate) > 0) {
      best_candidate = it;
    }
  }
  cit_ = best_candidate;
}

void UnsortedVectorRep::Get(const LookupKey& k, void* callback_args,
                            bool (*callback_func)(void* arg,
                                                  const char* entry)) {
#ifdef GET_TIMER
  auto start_copy = std::chrono::high_resolution_clock::now();
#endif
  rwlock_.ReadLock();
  UnsortedVectorRep* vector_rep;
  std::shared_ptr<Bucket> bucket;
  
  // [MODIFIED] Removed sorting logic .
  // We strictly just copy the bucket, regardless of immutability.
  if (immutable_) {
    vector_rep = this;
  } else {
    vector_rep = nullptr;
    bucket.reset(new Bucket(*bucket_));
  }

  UnsortedVectorRep::Iterator iter(vector_rep, immutable_ ? bucket_ : bucket,
                                   compare_);
  rwlock_.ReadUnlock();
  #ifdef GET_TIMER
  auto end_copy = std::chrono::high_resolution_clock::now();
  auto copy_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_copy - start_copy);
  std::cout << "Unsorted_Copy: " << copy_duration.count() << ", " << std::flush;
#endif

#ifdef GET_TIMER
  auto start_search = std::chrono::high_resolution_clock::now();
#endif

  // [NOTE] updated logic to perform a valid point lookup without sorting using new seek logic.
  for (iter.Seek(k.user_key(), k.memtable_key().data());
       iter.Valid() && callback_func(callback_args, iter.key()); iter.Next()) {
  }
  
  #ifdef GET_TIMER
  auto end_search = std::chrono::high_resolution_clock::now();
  auto search_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_search - start_search);
  std::cout << "Unsorted_Search: " << search_duration.count() << ", " << std::flush;
#endif
}

class AlwaysSortedVectorRep : public VectorRep {
public:
  AlwaysSortedVectorRep(const KeyComparator& compare, Allocator* allocator, size_t count) : VectorRep(compare, allocator, count) {

  }

  void Insert(KeyHandle handle) override {
#ifdef TIMER
    auto start = std::chrono::high_resolution_clock::now();
#endif

    auto* key = static_cast<char*>(handle);
    WriteLock l(&rwlock_);
    assert(!immutable_);
    const auto position = std::lower_bound(
      bucket_->begin(),
      bucket_->end(),
      key,
      [this](const char* a, const char* b) {
        return compare_(a, b) < 0;
      }
    );
    bucket_->insert(position, key);

#ifdef TIMER
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    std::cout << "AlwaysSortedVectorRep: " << duration.count() << ", " << std::flush;
#endif
  };

  void Get(const LookupKey &k, void *callback_args, bool (*callback_func)(void *arg, const char *entry)) override {
#ifdef GET_TIMER
    auto start_copy = std::chrono::high_resolution_clock::now();
#endif
    rwlock_.ReadLock();
    const auto vector_rep = this;
    const auto bucket = std::make_shared<Bucket>(*bucket_);

    AlwaysSortedVectorRep::Iterator iter(vector_rep, bucket,
                                         compare_);
    rwlock_.ReadUnlock();
      #ifdef GET_TIMER
    auto end_copy = std::chrono::high_resolution_clock::now();
    auto copy_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_copy - start_copy);
    std::cout << "Sorted_Copy: " << copy_duration.count() << ", " << std::flush;
#endif
#ifdef GET_TIMER
    auto start_search = std::chrono::high_resolution_clock::now();
#endif
    for (
      iter.Seek(k.user_key(), k.memtable_key().data());
      iter.Valid() && callback_func(callback_args, iter.key());
      iter.Next()
    ) {
    }
    #ifdef GET_TIMER
    auto end_search = std::chrono::high_resolution_clock::now();
    auto search_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_search - start_search);
    std::cout << "Sorted_Search: " << search_duration.count() << ", " << std::flush;
#endif
  }

  class Iterator : public MemTableRep::Iterator {
    std::shared_ptr<std::vector<const char*>> bucket_;
    std::vector<const char*>::const_iterator mutable cit_;
    const KeyComparator& compare_;
    std::string tmp_;  // For passing to EncodeKey

    public:
    Iterator(
      class AlwaysSortedVectorRep* vrep,
      std::shared_ptr<std::vector<const char*>> bucket,
      const KeyComparator& compare)
      : bucket_(bucket),
        cit_(bucket_->end()),
        compare_(compare) {}

    bool Valid() const override {
      return cit_ != bucket_->end();
    }
    const char* key() const override {return *cit_;}
    void Next() override {
      if (cit_ == bucket_->end()) {
        return;
      }
      ++cit_;
    }

    void Prev() override {
      if (cit_ == bucket_->begin()) {
        cit_ = bucket_->end();
      } else {
        --cit_;
      }
    }

    void Seek(const Slice& user_key, const char* memtable_key) override {
      const char* encoded_key =
          (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
      cit_ = std::equal_range(bucket_->begin(), bucket_->end(), encoded_key,
                              [this](const char* a, const char* b) {
                                return compare_(a, b) < 0;
                              })
                 .first;
    }

    void SeekForPrev(const Slice &internal_key, const char *memtable_key) override {
      assert(false);
    }
    void SeekToFirst() override {
      cit_ = bucket_->begin();
    }

    void SeekToLast() override {
      cit_ = bucket_->end();
      if (bucket_->size() != 0) {
        --cit_;
      }
    }
  };

  ~AlwaysSortedVectorRep() override = default;
};

}  // namespace

static std::unordered_map<std::string, OptionTypeInfo> vector_rep_table_info = {
    {"count",
     {0, OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    unsorted_vector_rep_table_info = {
        {"count",
         {0, OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    always_sorted_vector_rep_table_info = {
        {"count",
         {0, OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

VectorRepFactory::VectorRepFactory(size_t count) : count_(count) {
  RegisterOptions("VectorRepFactoryOptions", &count_, &vector_rep_table_info);
}

UnsortedVectorRepFactory::UnsortedVectorRepFactory(size_t count)
    : count_(count) {
  RegisterOptions("UnsortedVectorRepFactory", &count_,
                  &unsorted_vector_rep_table_info);
}

AlwaysSortedVectorRepFactory::AlwaysSortedVectorRepFactory(size_t count)
    : count_(count) {
  RegisterOptions("AlwaysSortedVectorRepFactory", &count_,
                  &always_sorted_vector_rep_table_info);
}

MemTableRep* VectorRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform*, Logger* /*logger*/) {
  return new VectorRep(compare, allocator, count_);
}

MemTableRep* UnsortedVectorRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform*, Logger* /* logger */) {
  return new UnsortedVectorRep(compare, allocator, count_);
}

MemTableRep* AlwaysSortedVectorRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform*, Logger* /* logger */) {
  return new AlwaysSortedVectorRep(compare, allocator, count_);
}
}  // namespace ROCKSDB_NAMESPACE