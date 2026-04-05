//  Custom implementation from SSD-Lab
//
//  LinkListRep is a MemTableRep implementation that uses a linked list.
//
#include <assert.h>
#include <stddef.h>

#include <algorithm>
#include <atomic>
#include <iostream>
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
  // Accessors/mutators for links. Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  LinkListNode* Next() {
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return next_.load(std::memory_order_acquire);
  }

  LinkListNode* Prev() {
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return prev_.load(std::memory_order_acquire);
  }

  void SetNext(LinkListNode* x) {
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_.store(x, std::memory_order_release);
  }
  void SetPrev(LinkListNode* x) {
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    prev_.store(x, std::memory_order_release);
  }

  LinkListNode* NoBarrier_Next() {
    return next_.load(std::memory_order_relaxed);
  }

  void NoBarrier_SetNext(LinkListNode* x) {
    next_.store(x, std::memory_order_relaxed);
  }

  LinkListNode* NoBarrier_Prev() {
    return prev_.load(std::memory_order_relaxed);
  }

  void NoBarrier_SetPrev(LinkListNode* x) {
    prev_.store(x, std::memory_order_relaxed);
  }

  LinkListNode() = default;

 private:
  std::atomic<LinkListNode*> next_;
  std::atomic<LinkListNode*> prev_;

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

  bool Contains(const char* key) const override;

  size_t ApproximateMemoryUsage() override;

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  bool KeyIsAtNode(const LinkListNode* a, const LinkListNode* b) const {
    // nullptr n is considered infinite
    return (a != nullptr && b != nullptr) && (compare_(a->key, b->key) == 0);
  }

  bool KeyIsAtNode(const Slice& user_key, const LinkListNode* n) const {
    return (n != nullptr) && (compare_(n->key, user_key) == 0);
  }

  LinkListNode* FindLatestOccuranceOfKey(LinkListNode* tail,
                                         const Slice& user_key) const {
    LinkListNode* x = tail;
    while (x != nullptr) {
      if (KeyIsAtNode(user_key, x)) {
        return x;
      }
      x = x->Prev();
    }
    return x;
  }

  ~LinkListRep() override = default;

  MemTableRep::Iterator* GetIterator(Arena* arena) override;

  class Iterator : public MemTableRep::Iterator {
   public:
    explicit Iterator(LinkListRep* link_list_rep, LinkListNode* head,
                      LinkListNode* tail, bool need_sorting = false)
        : link_list_rep_(link_list_rep),
          head_(head),
          tail_(tail),
          node_(nullptr),
          need_sorting_(need_sorting),
          sorted_(!need_sorting) {
      if (need_sorting_ && !sorted_) {
        MaybeSortLinkList();
      }
    }

    ~Iterator() override = default;

    bool Valid() const override { return node_ != nullptr && sorted_; }

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

    // void Seek(const Slice& user_key, const char* memtable_key) override {
    //   MaybeSortLinkList();
    //   const char* encoded_key =
    //       (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
    //   node_ = link_list_rep_->FindLatestOccuranceOfKey(tail_, encoded_key);
    // }
  void Seek(const Slice& user_key, const char* memtable_key) {
    const char* encoded_key =
        (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);

    LinkListNode* curr = head_; 
    while (curr != nullptr) {
      // Identical comparison to VectorRep's equal_range
      if (link_list_rep_->compare_(curr->key, encoded_key) >= 0) {
        break;
      }
      curr = curr->Next();
    }
    node_ = curr;
  }

    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
    }

    void SeekToFirst() override {
      MaybeSortLinkList();
      node_ = head_;
    }

    void SeekToLast() override {
      MaybeSortLinkList();
      node_ = tail_;
    }

   private:
    LinkListRep* link_list_rep_;
    LinkListNode* head_;
    LinkListNode* tail_;
    LinkListNode* node_;
    bool need_sorting_;
    bool sorted_;
    std::string tmp_;

    static LinkListNode* MergeSortedLists(
        LinkListNode* a, LinkListNode* b,
        const MemTableRep::KeyComparator& compare) {
      if (!a) return b;
      if (!b) return a;

      LinkListNode* head = nullptr;

      // Initialize head
      if (compare(a->key, b->key) < 0) {
        head = a;
        a = a->NoBarrier_Next();
      } else {
        head = b;
        b = b->NoBarrier_Next();
      }

      LinkListNode* curr = head;

      while (a && b) {
        if (compare(a->key, b->key) < 0) {
          curr->NoBarrier_SetNext(a);
          a->NoBarrier_SetPrev(curr);
          curr = a;
          a = a->NoBarrier_Next();
        } else {
          curr->NoBarrier_SetNext(b);
          b->NoBarrier_SetPrev(curr);
          curr = b;
          b = b->NoBarrier_Next();
        }
      }

      LinkListNode* remainder = a ? a : b;
      while (remainder) {
        curr->NoBarrier_SetNext(remainder);
        remainder->NoBarrier_SetPrev(curr);
        curr = remainder;
        remainder = remainder->NoBarrier_Next();
      }

      head->NoBarrier_SetPrev(nullptr);
      return head;
    }

    static LinkListNode* SplitList(LinkListNode* head) {
      LinkListNode* slow = head;
      LinkListNode* fast = head;

      while (fast->NoBarrier_Next() &&
             fast->NoBarrier_Next()->NoBarrier_Next()) {
        slow = slow->NoBarrier_Next();
        fast = fast->NoBarrier_Next()->NoBarrier_Next();
      }

      LinkListNode* second = slow->NoBarrier_Next();
      slow->NoBarrier_SetNext(nullptr);
      if (second) second->NoBarrier_SetPrev(nullptr);

      return second;
    }

    LinkListNode* MergeSort(LinkListNode* head,
                            const MemTableRep::KeyComparator& compare) {
      if (!head || !head->NoBarrier_Next()) return head;

      LinkListNode* second = SplitList(head);

      LinkListNode* left = MergeSort(head, compare);
      LinkListNode* right = MergeSort(second, compare);

      return MergeSortedLists(left, right, compare);
    }

    void MaybeSortLinkList() {
      if (!sorted_ && need_sorting_) {
        head_ = MergeSort(head_, link_list_rep_->compare_);

        tail_ = head_;
        while (tail_ && tail_->NoBarrier_Next()) {
          tail_ = tail_->NoBarrier_Next();
        }

        sorted_ = true;
      }
    }
  };

 private:
  std::atomic<LinkListNode*> head_;
  std::atomic<LinkListNode*> tail_;
  const KeyComparator& compare_;
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
  x->NoBarrier_SetNext(nullptr);
  x->NoBarrier_SetPrev(nullptr);
  *buf = x->key;
  return static_cast<void*>(x);
}

void LinkListRep::Insert(KeyHandle handle) {
  LinkListNode* node = reinterpret_cast<LinkListNode*>(handle);
  LinkListNode* curr_head = head_.load(std::memory_order_relaxed);

  if (curr_head == nullptr) {
    node->NoBarrier_SetNext(nullptr);
    node->NoBarrier_SetPrev(nullptr);
    head_.store(node, std::memory_order_release);
    tail_.store(node, std::memory_order_release);
    return;
  }

  if (compare_(node->key, curr_head->key) < 0) {
    node->NoBarrier_SetNext(curr_head);
    node->NoBarrier_SetPrev(nullptr);
    curr_head->SetPrev(node);
    head_.store(node, std::memory_order_release);
    return;
  }

  LinkListNode* curr = curr_head;
  while (curr->Next() != nullptr &&
         compare_(curr->Next()->key, node->key) < 0) {
    curr = curr->Next();
  }

  LinkListNode* next_node = curr->Next();
  node->NoBarrier_SetNext(next_node);
  node->NoBarrier_SetPrev(curr);

  if (next_node != nullptr) {
    next_node->SetPrev(node);
  } else {
    tail_.store(node, std::memory_order_release);
  }
  curr->SetNext(node);
}

// void LinkListRep::Insert(KeyHandle handle) {
//   LinkListNode* node = reinterpret_cast<LinkListNode*>(handle);
//   LinkListNode* old_tail = tail_.load(std::memory_order_relaxed);

//   if (old_tail == nullptr) {
//     node->NoBarrier_SetNext(nullptr);
//     node->NoBarrier_SetPrev(nullptr);
//     head_.store(node, std::memory_order_release);
//     tail_.store(node, std::memory_order_release);
//   } else {
//     node->NoBarrier_SetPrev(old_tail);
//     node->NoBarrier_SetNext(nullptr);
//     old_tail->SetNext(node);
//     tail_.store(node, std::memory_order_release);
//   }
// }

// bool LinkListRep::Contains(const char* key) const {
//   Slice internal_key = GetLengthPrefixedSlice(key);
//   LinkListNode* current = tail_.load(std::memory_order_acquire);
//   while (current != nullptr) {
//     if (compare_(current->key, internal_key) == 0) {
//       return true;
//     }
//     current = current->Prev();
//   }
//   return false;
// }

bool LinkListRep::Contains(const char* key) const {
  Slice internal_key = GetLengthPrefixedSlice(key);
  LinkListNode* current = tail_.load(std::memory_order_acquire);
  while (current != nullptr) {
    // DEBUG: Print both keys to see if they actually match visually
    std::cout << "Comparing: " << Slice(current->key).data() << " with "
              << internal_key.data() << std::endl;
    if (compare_(current->key, internal_key) == 0) {
      return true;
    }
    current = current->Prev();
  }
  return false;
}

size_t LinkListRep::ApproximateMemoryUsage() { return 0; }

void LinkListRep::Get(const LookupKey& k, void* callback_args,
                      bool (*callback_func)(void* arg, const char* entry)) {
  std::cout << "Contains Check: " << Contains(k.memtable_key().data())
            << std::endl
            << std::flush;
  if (head_ != nullptr) {
    Iterator iter(this, head_, tail_);

    for (iter.Seek(k.user_key(), k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, iter.key());
         iter.Next()) {
    }
  }
}

MemTableRep::Iterator* LinkListRep::GetIterator(Arena* arena) {
  std::cout << "CREATING AN ITERATOR HERE" << std::endl << std::flush;
  char* mem = nullptr;
  if (arena != nullptr) {
    mem = arena->AllocateAligned(sizeof(Iterator));
  }

  if (arena == nullptr) {
    return new Iterator(this, head_, tail_, true);
  } else {
    return new (mem) Iterator(this, head_, tail_, true);
  }
}
}  // namespace

MemTableRep* LinkListRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* /*transform*/, Logger* /*logger*/) {
  return new LinkListRep(compare, allocator);
}

}  // namespace ROCKSDB_NAMESPACE
