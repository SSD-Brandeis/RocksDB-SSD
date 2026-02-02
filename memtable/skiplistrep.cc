//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <random>
#include <vector>
#include <atomic>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/inlineskiplist.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"
#include "util/mutexlock.h"
#include "port/port.h"

#include <chrono>
#include <iostream>
// #define TIMER
namespace ROCKSDB_NAMESPACE {
namespace {
class SkipListRep : public MemTableRep {
  InlineSkipList<const MemTableRep::KeyComparator&> skip_list_;
  const MemTableRep::KeyComparator& cmp_;
  const SliceTransform* transform_;
  const size_t lookahead_;

  friend class LookaheadIterator;

 public:
  explicit SkipListRep(const MemTableRep::KeyComparator& compare,
                       Allocator* allocator, const SliceTransform* transform,
                       const size_t lookahead)
      : MemTableRep(allocator),
        skip_list_(compare, allocator),
        cmp_(compare),
        transform_(transform),
        lookahead_(lookahead) {}

  KeyHandle Allocate(const size_t len, char** buf) override {
    *buf = skip_list_.AllocateKey(len);
    return static_cast<KeyHandle>(*buf);
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(KeyHandle handle) override {
    // std::cout << "Insert Called!" << std::endl;
    #ifdef TIMER
    auto start = std::chrono::high_resolution_clock::now();
    #endif
    skip_list_.Insert(static_cast<char*>(handle));
    #ifdef TIMER
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    std::cout << "SkipList_Insert: " << duration.count() << ", " << std::flush;
    #endif
  }

  bool InsertKey(KeyHandle handle) override {
    // std::cout << "InsertKey called!" << std::endl;
    #ifdef TIMER
    auto start = std::chrono::high_resolution_clock::now();
    #endif
    auto res = skip_list_.Insert(static_cast<char*>(handle));
    #ifdef TIMER
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    std::cout << "SkipList_InsertKey: " << duration.count() << ", " << std::flush;
    #endif
    return res;
  }



  void InsertWithHint(KeyHandle handle, void** hint) override {
    // std::cout << "InsertWithHint called!" << std::endl;
    skip_list_.InsertWithHint(static_cast<char*>(handle), hint);
  }

  bool InsertKeyWithHint(KeyHandle handle, void** hint) override {
    return skip_list_.InsertWithHint(static_cast<char*>(handle), hint);
  }

  void InsertWithHintConcurrently(KeyHandle handle, void** hint) override {
    skip_list_.InsertWithHintConcurrently(static_cast<char*>(handle), hint);
  }

  bool InsertKeyWithHintConcurrently(KeyHandle handle, void** hint) override {
    return skip_list_.InsertWithHintConcurrently(static_cast<char*>(handle),
                                                 hint);
  }

  void InsertConcurrently(KeyHandle handle) override {
    skip_list_.InsertConcurrently(static_cast<char*>(handle));
  }

  bool InsertKeyConcurrently(KeyHandle handle) override {
    return skip_list_.InsertConcurrently(static_cast<char*>(handle));
  }

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const char* key) const override {
    return skip_list_.Contains(key);
  }

  size_t ApproximateMemoryUsage() override {
    // All memory is allocated through allocator; nothing to report here
    return 0;
  }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {

    SkipListRep::Iterator iter(&skip_list_);
    Slice dummy_slice;
    for (iter.Seek(dummy_slice, k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, iter.key());
         iter.Next()) {
    }
  }

  Status GetAndValidate(const LookupKey& k, void* callback_args,
                        bool (*callback_func)(void* arg, const char* entry),
                        bool allow_data_in_errors) override {
    SkipListRep::Iterator iter(&skip_list_);
    Slice dummy_slice;
    Status status = iter.SeekAndValidate(dummy_slice, k.memtable_key().data(),
                                         allow_data_in_errors);
    for (; iter.Valid() && status.ok() &&
           callback_func(callback_args, iter.key());
         status = iter.NextAndValidate(allow_data_in_errors)) {
    }
    return status;
  }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    return skip_list_.ApproximateNumEntries(start_ikey, end_ikey);
  }

  void UniqueRandomSample(const uint64_t num_entries,
                          const uint64_t target_sample_size,
                          std::unordered_set<const char*>* entries) override {
    entries->clear();
    // Avoid divide-by-0.
    assert(target_sample_size > 0);
    assert(num_entries > 0);
    // NOTE: the size of entries is not enforced to be exactly
    // target_sample_size at the end of this function, it might be slightly
    // greater or smaller.
    SkipListRep::Iterator iter(&skip_list_);
    // There are two methods to create the subset of samples (size m)
    // from the table containing N elements:
    // 1-Iterate linearly through the N memtable entries. For each entry i,
    //   add it to the sample set with a probability
    //   (target_sample_size - entries.size() ) / (N-i).
    //
    // 2-Pick m random elements without repetition.
    // We pick Option 2 when m<sqrt(N) and
    // Option 1 when m > sqrt(N).
    if (target_sample_size >
        static_cast<uint64_t>(std::sqrt(1.0 * num_entries))) {
      Random* rnd = Random::GetTLSInstance();
      iter.SeekToFirst();
      uint64_t counter = 0, num_samples_left = target_sample_size;
      for (; iter.Valid() && (num_samples_left > 0); iter.Next(), counter++) {
        // Add entry to sample set with probability
        // num_samples_left/(num_entries - counter).
        if (rnd->Next() % (num_entries - counter) < num_samples_left) {
          entries->insert(iter.key());
          num_samples_left--;
        }
      }
    } else {
      // Option 2: pick m random elements with no duplicates.
      // If Option 2 is picked, then target_sample_size<sqrt(N)
      // Using a set spares the need to check for duplicates.
      for (uint64_t i = 0; i < target_sample_size; i++) {
        // We give it 5 attempts to find a non-duplicate
        // With 5 attempts, the chances of returning `entries` set
        // of size target_sample_size is:
        // PROD_{i=1}^{target_sample_size-1} [1-(i/N)^5]
        // which is monotonically increasing with N in the worse case
        // of target_sample_size=sqrt(N), and is always >99.9% for N>4.
        // At worst, for the final pick , when m=sqrt(N) there is
        // a probability of p= 1/sqrt(N) chances to find a duplicate.
        for (uint64_t j = 0; j < 5; j++) {
          iter.RandomSeek();
          // unordered_set::insert returns pair<iterator, bool>.
          // The second element is true if an insert successfully happened.
          // If element is already in the set, this bool will be false, and
          // true otherwise.
          if ((entries->insert(iter.key())).second) {
            break;
          }
        }
      }
    }
  }

  ~SkipListRep() override = default;

  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
    InlineSkipList<const MemTableRep::KeyComparator&>::Iterator iter_;

   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(
        const InlineSkipList<const MemTableRep::KeyComparator&>* list)
        : iter_(list) {}

    ~Iterator() override = default;

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override { return iter_.Valid(); }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override {
      assert(Valid());
      return iter_.key();
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override {
      assert(Valid());
      iter_.Next();
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() override {
      assert(Valid());
      iter_.Prev();
    }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.Seek(memtable_key);
      } else {
        iter_.Seek(EncodeKey(&tmp_, user_key));
      }
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
      iter_.SeekForPrev(memtable_key);
      } else {
      iter_.SeekForPrev(EncodeKey(&tmp_, user_key));
      }
    }

    void RandomSeek() override { iter_.RandomSeek(); }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() override { iter_.SeekToFirst(); }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast() override { iter_.SeekToLast(); }

    Status NextAndValidate(bool allow_data_in_errors) override {
      assert(Valid());
      return iter_.NextAndValidate(allow_data_in_errors);
    }

    Status SeekAndValidate(const Slice& user_key, const char* memtable_key,
                           bool allow_data_in_errors) override {
      if (memtable_key != nullptr) {
        return iter_.SeekAndValidate(memtable_key, allow_data_in_errors);
      } else {
        return iter_.SeekAndValidate(EncodeKey(&tmp_, user_key),
                                     allow_data_in_errors);
      }
    }

    Status PrevAndValidate(bool allow_data_in_error) override {
      assert(Valid());
      return iter_.PrevAndValidate(allow_data_in_error);
    }

   protected:
    std::string tmp_;  // For passing to EncodeKey
  };

  // Iterator over the contents of a skip list which also keeps track of the
  // previously visited node. In Seek(), it examines a few nodes after it
  // first, falling back to O(log n) search from the head of the list only if
  // the target key hasn't been found.
  class LookaheadIterator : public MemTableRep::Iterator {
   public:
    explicit LookaheadIterator(const SkipListRep& rep)
        : rep_(rep), iter_(&rep_.skip_list_), prev_(iter_) {}

    ~LookaheadIterator() override = default;

    bool Valid() const override { return iter_.Valid(); }

    const char* key() const override {
      assert(Valid());
      return iter_.key();
    }

    void Next() override {
      assert(Valid());

      bool advance_prev = true;
      if (prev_.Valid()) {
        auto k1 = rep_.UserKey(prev_.key());
        auto k2 = rep_.UserKey(iter_.key());

        if (k1.compare(k2) == 0) {
          // same user key, don't move prev_
          advance_prev = false;
        } else if (rep_.transform_) {
          // only advance prev_ if it has the same prefix as iter_
          auto t1 = rep_.transform_->Transform(k1);
          auto t2 = rep_.transform_->Transform(k2);
          advance_prev = t1.compare(t2) == 0;
        }
      }

      if (advance_prev) {
        prev_ = iter_;
      }
      iter_.Next();
    }

    void Prev() override {
      assert(Valid());
      iter_.Prev();
      prev_ = iter_;
    }

    void Seek(const Slice& internal_key, const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);

      if (prev_.Valid() && rep_.cmp_(encoded_key, prev_.key()) >= 0) {
        // prev_.key() is smaller or equal to our target key; do a quick
        // linear search (at most lookahead_ steps) starting from prev_
        iter_ = prev_;

        size_t cur = 0;
        while (cur++ <= rep_.lookahead_ && iter_.Valid()) {
          if (rep_.cmp_(encoded_key, iter_.key()) <= 0) {
            return;
          }
          Next();
        }
      }

      iter_.Seek(encoded_key);
      prev_ = iter_;
    }

    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);
      iter_.SeekForPrev(encoded_key);
      prev_ = iter_;
    }

    void SeekToFirst() override {
      iter_.SeekToFirst();
      prev_ = iter_;
    }

    void SeekToLast() override {
      iter_.SeekToLast();
      prev_ = iter_;
    }

   protected:
    std::string tmp_;  // For passing to EncodeKey

   private:
    const SkipListRep& rep_;
    InlineSkipList<const MemTableRep::KeyComparator&>::Iterator iter_;
    InlineSkipList<const MemTableRep::KeyComparator&>::Iterator prev_;
  };

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    if (lookahead_ > 0) {
      void* mem =
          arena ? arena->AllocateAligned(sizeof(SkipListRep::LookaheadIterator))
                : operator new(sizeof(SkipListRep::LookaheadIterator));
      return new (mem) SkipListRep::LookaheadIterator(*this);
    } else {
      void* mem = arena ? arena->AllocateAligned(sizeof(SkipListRep::Iterator))
                        : operator new(sizeof(SkipListRep::Iterator));
      return new (mem) SkipListRep::Iterator(&skip_list_);
    }
  }
};


// SIMPLE SKIP LIST 

class SimpleSkipList {
 public:
  static const int kMaxHeight = 12;

  struct Node {
    const char* key;
    Node* next[kMaxHeight];

    explicit Node(const char* k) : key(k) {
      for (int i = 0; i < kMaxHeight; ++i) {
        next[i] = nullptr;
      }
    }
  };

  SimpleSkipList(const MemTableRep::KeyComparator& compare, Allocator* allocator)
      : compare_(compare),
        allocator_(allocator),
        max_height_(1) {
    head_ = new Node(nullptr);
    unsigned int seed = static_cast<unsigned int>(std::chrono::system_clock::now().time_since_epoch().count());
    rnd_.seed(seed);
  }

  const MemTableRep::KeyComparator& Compare() const { return compare_; }

  ~SimpleSkipList() {
    Node* curr = head_;
    while (curr != nullptr) {
      Node* next = curr->next[0];
      delete curr;
      curr = next;
    }
  }

  void Insert(const char* key) {
    Node* update[kMaxHeight];
    Node* x = head_;
    
    for (int i = max_height_ - 1; i >= 0; i--) {
      while (true) {
        Node* next = x->next[i];
        if (next != nullptr && compare_(next->key, key) < 0) {
          x = next;
        } else {
          break;
        }
      }
      update[i] = x;
    }

    int height = RandomHeight();
    
    if (height > max_height_) {
      for (int i = max_height_; i < height; i++) {
        update[i] = head_;
      }
      max_height_ = height;
    }

    Node* new_node = new Node(key);
    for (int i = 0; i < height; i++) {
      new_node->next[i] = update[i]->next[i];
      update[i]->next[i] = new_node;
    }
  }

  bool Contains(const char* key) const {
    Node* x = FindGreaterOrEqual(key);
    if (x != nullptr && compare_(x->key, key) == 0) {
      return true;
    }
    return false;
  }

  Node* FindGreaterOrEqual(const char* key) const {
    Node* x = head_;
    int level = max_height_ - 1;
    while (true) {
      Node* next = x->next[level];
      if (next != nullptr && compare_(next->key, key) < 0) {
        x = next;
      } else {
        if (level == 0) {
          return next;
        } else {
          level--;
        }
      }
    }
  }

  Node* FindLessThan(const char* key) const {
    Node* x = head_;
    int level = max_height_ - 1;
    while (true) {
      Node* next = x->next[level];
      if (next != nullptr && compare_(next->key, key) < 0) {
        x = next;
      } else {
        if (level == 0) {
          return x;
        } else {
          level--;
        }
      }
    }
  }

  Node* FindLast() const {
    Node* x = head_;
    int level = max_height_ - 1;
    while (true) {
      Node* next = x->next[level];
      if (next == nullptr) {
        if (level == 0) {
          return x;
        } else {
          level--;
        }
      } else {
        x = next;
      }
    }
  }

  Node* GetHead() const { return head_; }

 private:
  int RandomHeight() {
    int height = 1;
    while (height < kMaxHeight && (rnd_() % 4) == 0) {
      height++;
    }
    return height;
  }

  const MemTableRep::KeyComparator& compare_;
  Allocator* allocator_;
  Node* head_;
  int max_height_;
  std::mt19937 rnd_;
};


// SimpleSkipListRep

class SimpleSkipListRep : public MemTableRep {
 public:
  SimpleSkipListRep(const KeyComparator& compare, Allocator* allocator)
      : MemTableRep(allocator),
        skip_list_(compare, allocator) {}

  void Insert(KeyHandle handle) override {
#ifdef TIMER
    auto start = std::chrono::high_resolution_clock::now();
#endif
    WriteLock l(&rwlock_);
    auto* key = static_cast<char*>(handle);
    skip_list_.Insert(key);
#ifdef TIMER
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    std::cout << "SimpleSkipListRep::Insert: " << duration.count() << std::endl;
#endif
  }

  bool Contains(const char* key) const override {
    ReadLock l(&rwlock_);
    return skip_list_.Contains(key);
  }

  size_t ApproximateMemoryUsage() override { return 0; }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
#ifdef TIMER
    auto start = std::chrono::high_resolution_clock::now();
#endif
    ReadLock l(&rwlock_);
    SimpleSkipListRep::Iterator iter(&skip_list_);
    for (iter.Seek(k.user_key(), k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, iter.key()); iter.Next()) {
    }
#ifdef TIMER
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    std::cout << "SimpleSkipListRep::Get: " << duration.count() << std::endl;
#endif
  }

  ~SimpleSkipListRep() override = default;

  class Iterator : public MemTableRep::Iterator {
    SimpleSkipList* list_;
    SimpleSkipList::Node* node_;
    std::string tmp_;

   public:
    explicit Iterator(SimpleSkipList* list)
        : list_(list), node_(nullptr) {}

    bool Valid() const override {
      return node_ != nullptr;
    }

    const char* key() const override {
      assert(Valid());
      return node_->key;
    }

    void Next() override {
      assert(Valid());
      node_ = node_->next[0];
    }

    void Prev() override {
      assert(Valid());
      node_ = list_->FindLessThan(node_->key);
      if (node_ == list_->GetHead()) {
        node_ = nullptr;
      }
    }

    void Seek(const Slice& user_key, const char* memtable_key) override {
      const char* encoded_key =
          (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
      node_ = list_->FindGreaterOrEqual(encoded_key);
    }

    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      const char* encoded_key =
          (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
      Seek(user_key, encoded_key);
      if (!Valid()) {
        SeekToLast();
      }
      while (Valid() && list_->FindGreaterOrEqual(encoded_key) == node_) {
         node_ = list_->FindLessThan(encoded_key);
         if(node_ == list_->GetHead()) node_ = nullptr;
         SimpleSkipList::Node* exact = list_->FindGreaterOrEqual(encoded_key);
         if (exact && list_->Compare()(exact->key, encoded_key) == 0) { 
             node_ = exact;
         }
         return;
      }
    }

    void SeekToFirst() override {
      node_ = list_->GetHead()->next[0];
    }

    void SeekToLast() override {
      node_ = list_->FindLast();
      if (node_ == list_->GetHead()) {
        node_ = nullptr;
      }
    }
  };

  MemTableRep::Iterator* GetIterator(Arena* arena) override {
    if (arena != nullptr) {
      void* mem = arena->AllocateAligned(sizeof(Iterator));
      return new (mem) Iterator(&skip_list_);
    } else {
      return new Iterator(&skip_list_);
    }
  }

 protected:
  SimpleSkipList skip_list_;
  mutable port::RWMutex rwlock_;
};
}  // namespace

static std::unordered_map<std::string, OptionTypeInfo> skiplist_factory_info = {
    {"lookahead",
     {0, OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kDontSerialize /*Since it is part of the ID*/}},
};

SkipListFactory::SkipListFactory(size_t lookahead) : lookahead_(lookahead) {
  RegisterOptions("SkipListFactoryOptions", &lookahead_,
                  &skiplist_factory_info);
}

std::string SkipListFactory::GetId() const {
  std::string id = Name();
  if (lookahead_ > 0) {
    id.append(":").append(std::to_string(lookahead_));
  }
  return id;
}

MemTableRep* SkipListFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* transform, Logger* /*logger*/) {
  return new SkipListRep(compare, allocator, transform, lookahead_);
}


// SimpleSkipListFactory

class SimpleSkipListFactory : public MemTableRepFactory {
 public:
  SimpleSkipListFactory() {}

  using MemTableRepFactory::CreateMemTableRep;

  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* /*transform*/,
                                 Logger* /*logger*/) override {
    return new SimpleSkipListRep(compare, allocator);
  }

  const char* Name() const override { return "SimpleSkipListRepFactory"; }
};

MemTableRepFactory* NewSimpleSkipListRepFactory() {
  return new SimpleSkipListFactory();
}

}  // namespace ROCKSDB_NAMESPACE