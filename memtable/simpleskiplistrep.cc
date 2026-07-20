//  Custom implementation from SSD-Lab
//
//  SimpleSkipListRep is a MemTableRep implementation that uses a simple skip
//  list (classic design, without the inline-key and splice optimizations of
//  RocksDB's InlineSkipList).
//
//  Concurrency control (lock-free for both sides):
//   - Readers (Contains/Get/iterators) are lock-free. Next pointers are
//     std::atomic<Node*>; writers publish a node with release stores/CAS and
//     readers observe them with acquire loads, so a reader either sees the
//     fully-initialized node or does not see it at all. Nodes are never
//     deleted while the memtable is alive (arena-backed), so no reclamation
//     protocol is needed.
//   - Writers use per-level compare-and-swap insertion (the standard
//     concurrent skip-list insert, as in RocksDB's InlineSkipList): compute
//     the splice, link each level bottom-up with CAS, and re-search a level
//     on CAS failure. Because keys are unique and nothing is removed, the
//     structure is ABA-free and insert throughput scales with the number of
//     write threads.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <random>
#include <vector>

#include "db/memtable.h"
#include "memory/arena.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/options_type.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class SimpleSkipList {
 public:
  static const int kMaxHeight = 12;

  struct Node {
    const char* key;
    int height;
    std::atomic<Node*> next[1];  // flexible trailing array — actual size
                                 // determined at allocation

    Node* Next(int level) const {
      return next[level].load(std::memory_order_acquire);
    }
    void SetNext(int level, Node* x) {
      next[level].store(x, std::memory_order_release);
    }
    bool CASNext(int level, Node* expected, Node* x) {
      return next[level].compare_exchange_strong(expected, x,
                                                 std::memory_order_release,
                                                 std::memory_order_relaxed);
    }
    // Safe before the node is published.
    void NoBarrier_SetNext(int level, Node* x) {
      next[level].store(x, std::memory_order_relaxed);
    }

    static Node* Create(const char* k, int h, Allocator* alloc) {
      size_t sz = sizeof(Node) + (h - 1) * sizeof(std::atomic<Node*>);
      char* mem = alloc->AllocateAligned(sz);
      Node* n = new (mem) Node();
      n->key = k;
      n->height = h;
      for (int i = 0; i < h; ++i) {
        // Slot 0 is constructed by the placement-new above; construct the
        // trailing raw slots in place before use.
        if (i > 0) {
          new (&n->next[i]) std::atomic<Node*>(nullptr);
        } else {
          n->next[i].store(nullptr, std::memory_order_relaxed);
        }
      }
      return n;
    }
  };

  SimpleSkipList(const MemTableRep::KeyComparator& compare,
                 Allocator* allocator)
      : compare_(compare), allocator_(allocator), max_height_(1) {
    //  Allocate head node directly from RocksDB's Arena
    head_ = Node::Create(nullptr, kMaxHeight, allocator_);
  }

  const MemTableRep::KeyComparator& Compare() const { return compare_; }

  ~SimpleSkipList() {
    // arena will destroy it, just trust arena or this metadata will be
    // incorrectly small
  }

  // Lock-free insert; safe under any number of concurrent writers.
  void Insert(const char* key) {
    int height = RandomHeight();

    // Raise the list height first. A reader or writer that observes the new
    // height while head_->next[level] is still null simply descends a level.
    int max_h = max_height_.load(std::memory_order_relaxed);
    while (height > max_h &&
           !max_height_.compare_exchange_weak(max_h, height,
                                              std::memory_order_relaxed)) {
    }

    Node* preds[kMaxHeight];
    Node* succs[kMaxHeight];
    FindSplice(key, height, preds, succs);

    Node* new_node = Node::Create(key, height, allocator_);

    // Link bottom-up. Level 0 publication makes the node reachable; upper
    // levels are an accelerator and may lag briefly.
    for (int i = 0; i < height; i++) {
      while (true) {
        new_node->NoBarrier_SetNext(i, succs[i]);
        if (preds[i]->CASNext(i, succs[i], new_node)) {
          break;
        }
        // Lost a race at this level: re-derive pred/succ. preds[i] is still
        // reachable and strictly before key (nodes never move or vanish), so
        // resume the search from it.
        FindSpliceForLevel(key, preds[i], i, &preds[i], &succs[i]);
      }
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
    int level = GetMaxHeight() - 1;
    while (true) {
      Node* next = x->Next(level);
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
    int level = GetMaxHeight() - 1;
    while (true) {
      Node* next = x->Next(level);
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
    int level = GetMaxHeight() - 1;
    while (true) {
      Node* next = x->Next(level);
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
  int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  // Computes pred/succ at levels [0, height) for key. Uses the current list
  // height so the walk starts at the top accelerator lanes.
  void FindSplice(const char* key, int height, Node** preds, Node** succs) {
    Node* pred = head_;
    for (int level = GetMaxHeight() - 1; level >= 0; level--) {
      Node* next = pred->Next(level);
      while (next != nullptr && compare_(next->key, key) < 0) {
        pred = next;
        next = pred->Next(level);
      }
      if (level < height) {
        preds[level] = pred;
        succs[level] = next;
      }
    }
  }

  // Recomputes pred/succ at one level, resuming from a node known to be
  // before key.
  void FindSpliceForLevel(const char* key, Node* from, int level, Node** pred,
                          Node** succ) {
    Node* p = from;
    Node* next = p->Next(level);
    while (next != nullptr && compare_(next->key, key) < 0) {
      p = next;
      next = p->Next(level);
    }
    *pred = p;
    *succ = next;
  }

  int RandomHeight() {
    // Thread-local generator: RandomHeight is called concurrently by
    // parallel write-group threads.
    static thread_local std::mt19937 rnd(
        static_cast<unsigned int>(
            std::chrono::steady_clock::now().time_since_epoch().count()) ^
        static_cast<unsigned int>(
            std::hash<std::thread::id>{}(std::this_thread::get_id())));
    int height = 1;
    while (height < kMaxHeight && (rnd() % 4) == 0) {
      height++;
    }
    return height;
  }

  const MemTableRep::KeyComparator& compare_;
  Allocator* allocator_;
  Node* head_;
  std::atomic<int> max_height_;
};

class SimpleSkipListRep : public MemTableRep {
 public:
  SimpleSkipListRep(const KeyComparator& compare, Allocator* allocator)
      : MemTableRep(allocator), skip_list_(compare, allocator) {}

  void Insert(KeyHandle handle) override {
    auto* key = static_cast<char*>(handle);
    skip_list_.Insert(key);
  }

  // CAS-based insert is safe under parallel write-group threads.
  void InsertConcurrently(KeyHandle handle) override { Insert(handle); }

  bool Contains(const char* key) const override {
    return skip_list_.Contains(key);
  }

  //  returns 0 because all are tracked by the Arena
  size_t ApproximateMemoryUsage() override { return 0; }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
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
      node_ = node_->Next(0);
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
      // Position at the last entry with a key <= target.
      node_ = list_->FindGreaterOrEqual(encoded_key);
      if (node_ != nullptr &&
          list_->Compare()(node_->key, encoded_key) == 0) {
        return;
      }
      node_ = list_->FindLessThan(encoded_key);
      if (node_ == list_->GetHead()) {
        node_ = nullptr;
      }
    }

    void SeekToFirst() override { node_ = list_->GetHead()->Next(0); }

    void SeekToLast() override {
      node_ = list_->FindLast();
      if (node_ == list_->GetHead()) {
        node_ = nullptr;
      }
    }
  };

  MemTableRep::Iterator* GetIterator(Arena* arena) override {
    if (arena != nullptr) {
      char* mem = arena->AllocateAligned(sizeof(Iterator));
      return new (mem) Iterator(&skip_list_);
    } else {
      return new Iterator(&skip_list_);
    }
  }

 protected:
  SimpleSkipList skip_list_;
};
}  // namespace

SimpleSkipListFactory::SimpleSkipListFactory() {}

MemTableRep* SimpleSkipListFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* /*transform*/, Logger* /*logger*/) {
  return new SimpleSkipListRep(compare, allocator);
}

}  // namespace ROCKSDB_NAMESPACE
