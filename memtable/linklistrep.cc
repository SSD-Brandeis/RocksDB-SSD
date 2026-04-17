/*
  Custom implementation from SSD-Lab
    LinkListRep is a MemTableRep implementation that uses a sorted doubly-linked
  list. Insert maintains sorted order.
*/

#include <assert.h>
#include <stddef.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <vector>

#include "db/memtable.h"
#include "memory/arena.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "util/coding.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace {

struct LinkListNode {
  LinkListNode* Next() { return next_.load(std::memory_order_acquire); }
  LinkListNode* Prev() { return prev_.load(std::memory_order_acquire); }
  void SetNext(LinkListNode* x) { next_.store(x, std::memory_order_release); }
  void SetPrev(LinkListNode* x) { prev_.store(x, std::memory_order_release); }

  LinkListNode() = default;

 private:
  std::atomic<LinkListNode*> next_{nullptr};
  std::atomic<LinkListNode*> prev_{nullptr};

  LinkListNode(const LinkListNode&) = delete;
  LinkListNode& operator=(const LinkListNode&) = delete;

 public:
  char key[1];
};

class LinkListRep : public MemTableRep {
 public:
  explicit LinkListRep(const MemTableRep::KeyComparator& compare,
                       Allocator* allocator);

  KeyHandle Allocate(const size_t len, char** buf) override;

  void Insert(KeyHandle handle) override;

  void InsertConcurrently(KeyHandle handle) override;

  bool InsertKeyConcurrently(KeyHandle handle) override;

  bool Contains(const char* key) const override;

  size_t ApproximateMemoryUsage() override;

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~LinkListRep() override = default;

  MemTableRep::Iterator* GetIterator(Arena* arena) override;

  class Iterator : public MemTableRep::Iterator {
   public:
    explicit Iterator(const LinkListRep* rep, LinkListNode* head,
                      LinkListNode* tail)
        : rep_(rep), head_(head), tail_(tail), node_(nullptr) {}

    ~Iterator() override = default;

    bool Valid() const override { return node_ != nullptr; }

    const char* key() const override {
      assert(Valid());
      return node_->key;
    }

    void Next() override {
      assert(Valid());
      node_ = node_->Next();
    }

    void Prev() override {
      assert(Valid());
      node_ = node_->Prev();
    }

    void Seek(const Slice& user_key, const char* memtable_key) override {
      const char* encoded_key =
          (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);

      node_ = head_;
      while (node_ != nullptr) {
        if (rep_->compare_(node_->key, encoded_key) >= 0) {
          return;
        }
        node_ = node_->Next();
      }
    }

    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      const char* encoded_key =
          (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);

      node_ = head_;
      LinkListNode* candidate = nullptr;
      while (node_ != nullptr) {
        int cmp = rep_->compare_(node_->key, encoded_key);
        if (cmp == 0) {
          return;
        }
        if (cmp > 0) {
          break;
        }
        candidate = node_;
        node_ = node_->Next();
      }
      node_ = candidate;
    }

    void SeekToFirst() override { node_ = head_; }

    void SeekToLast() override { node_ = tail_; }

   private:
    const LinkListRep* rep_;
    LinkListNode* head_;
    LinkListNode* tail_;
    LinkListNode* node_;
    std::string tmp_;
  };

 private:
  friend class Iterator;
  std::atomic<LinkListNode*> head_;
  std::atomic<LinkListNode*> tail_;
  const KeyComparator& compare_;
  mutable port::RWMutex rwlock_;
};

LinkListRep::LinkListRep(const MemTableRep::KeyComparator& compare,
                         Allocator* allocator)
    : MemTableRep(allocator),
      head_(nullptr),
      tail_(nullptr),
      compare_(compare) {}

KeyHandle LinkListRep::Allocate(const size_t len, char** buf) {
  char* mem = allocator_->AllocateAligned(sizeof(LinkListNode) + len);
  LinkListNode* x = new (mem) LinkListNode();
  *buf = x->key;
  return static_cast<void*>(x);
}

void LinkListRep::Insert(KeyHandle handle) {
  WriteLock l(&rwlock_);
  LinkListNode* node = reinterpret_cast<LinkListNode*>(handle);
  LinkListNode* curr_head = head_.load(std::memory_order_relaxed);

  // Empty list.
  if (curr_head == nullptr) {
    node->SetNext(nullptr);
    node->SetPrev(nullptr);
    head_.store(node, std::memory_order_release);
    tail_.store(node, std::memory_order_release);
    return;
  }

  // Insert before head (new smallest key).
  if (compare_(node->key, curr_head->key) < 0) {
    node->SetNext(curr_head);
    node->SetPrev(nullptr);
    curr_head->SetPrev(node);
    head_.store(node, std::memory_order_release);
    return;
  }

  // Walk to insertion point: find last node whose key < node->key.
  LinkListNode* curr = curr_head;
  while (curr->Next() != nullptr &&
         compare_(curr->Next()->key, node->key) < 0) {
    curr = curr->Next();
  }

  LinkListNode* next_node = curr->Next();
  node->SetNext(next_node);
  node->SetPrev(curr);

  if (next_node != nullptr) {
    next_node->SetPrev(node);
  } else {
    tail_.store(node, std::memory_order_release);
  }
  curr->SetNext(node);
}

void LinkListRep::InsertConcurrently(KeyHandle handle) { Insert(handle); }

bool LinkListRep::InsertKeyConcurrently(KeyHandle handle) {
  InsertConcurrently(handle);
  return true;
}

bool LinkListRep::Contains(const char* key) const {
  ReadLock l(&rwlock_);
  Slice internal_key = GetLengthPrefixedSlice(key);
  LinkListNode* current = head_.load(std::memory_order_relaxed);
  while (current != nullptr) {
    int cmp = compare_(current->key, internal_key);
    if (cmp == 0) {
      return true;
    }
    if (cmp > 0) {
      // Past the point where key could be — list is sorted.
      break;
    }
    current = current->Next();
  }
  return false;
}

size_t LinkListRep::ApproximateMemoryUsage() { return 0; }

void LinkListRep::Get(const LookupKey& k, void* callback_args,
                      bool (*callback_func)(void* arg, const char* entry)) {
  ReadLock l(&rwlock_);
  const char* encoded_key = k.memtable_key().data();
  LinkListNode* curr = head_.load(std::memory_order_relaxed);

  // Seek to first node >= encoded_key.
  while (curr != nullptr) {
    if (compare_(curr->key, encoded_key) >= 0) {
      break;
    }
    curr = curr->Next();
  }

  // Iterate forward, invoking the callback.
  while (curr != nullptr) {
    if (!callback_func(callback_args, curr->key)) {
      break;
    }
    curr = curr->Next();
  }
}

// Snapshot head/tail so the iterator has a stable starting view.
// The iterator itself does not hold any lock across calls
MemTableRep::Iterator* LinkListRep::GetIterator(Arena* arena) {
  // Acquire read lock to get a consistent (head, tail) pair.
  ReadLock l(&rwlock_);
  LinkListNode* h = head_.load(std::memory_order_relaxed);
  LinkListNode* t = tail_.load(std::memory_order_relaxed);

  if (arena != nullptr) {
    char* mem = arena->AllocateAligned(sizeof(Iterator));
    return new (mem) Iterator(this, h, t);
  }
  return new Iterator(this, h, t);
}
}  // namespace

LinkListRepFactory::LinkListRepFactory() {}

MemTableRep* LinkListRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* /*transform*/, Logger* /*logger*/) {
  return new LinkListRep(compare, allocator);
}

}  // namespace ROCKSDB_NAMESPACE
