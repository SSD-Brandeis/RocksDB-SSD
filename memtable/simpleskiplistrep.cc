//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <atomic>
#include <chrono>
#include <random>
#include <vector>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/inlineskiplist.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/options_type.h"
#include "util/mutexlock.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {

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

  SimpleSkipList(const MemTableRep::KeyComparator& compare,
                 Allocator* allocator)
      : compare_(compare), allocator_(allocator), max_height_(1) {
    head_ = new Node(nullptr);
    unsigned int seed = static_cast<unsigned int>(
        std::chrono::system_clock::now().time_since_epoch().count());
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

class SimpleSkipListRep : public MemTableRep {
 public:
  SimpleSkipListRep(const KeyComparator& compare, Allocator* allocator)
      : MemTableRep(allocator), skip_list_(compare, allocator) {}

  void Insert(KeyHandle handle) override {
    WriteLock l(&rwlock_);
    auto* key = static_cast<char*>(handle);
    skip_list_.Insert(key);
  }

  bool Contains(const char* key) const override {
    ReadLock l(&rwlock_);
    return skip_list_.Contains(key);
  }

  size_t ApproximateMemoryUsage() override { return 0; }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    ReadLock l(&rwlock_);
    SimpleSkipListRep::Iterator iter(&skip_list_);
    for (iter.Seek(k.user_key(), k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, iter.key());
         iter.Next()) {
    }
  }

  ~SimpleSkipListRep() override = default;

  class Iterator : public MemTableRep::Iterator {
    SimpleSkipList* list_;
    SimpleSkipList::Node* node_;
    std::string tmp_;

   public:
    explicit Iterator(SimpleSkipList* list) : list_(list), node_(nullptr) {}

    bool Valid() const override { return node_ != nullptr; }

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
        if (node_ == list_->GetHead()) node_ = nullptr;
        SimpleSkipList::Node* exact = list_->FindGreaterOrEqual(encoded_key);
        if (exact && list_->Compare()(exact->key, encoded_key) == 0) {
          node_ = exact;
        }
        return;
      }
    }

    void SeekToFirst() override { node_ = list_->GetHead()->next[0]; }

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
