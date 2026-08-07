// btree_rep.cc
//

#ifndef ROCKSDB_LITE

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memory/arena.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"

namespace rocksdb {

namespace {

class NodeHeader {
 public:
  std::atomic<uint64_t> version_lock_obsolete{0b00};
  uint16_t count = 0;
  const bool is_leaf;

  explicit NodeHeader(bool leaf) : is_leaf(leaf) {}

  static bool isLocked(uint64_t version) { return (version & 0b10) == 0b10; }
  static bool isObsolete(uint64_t version) { return (version & 1) == 1; }


  uint64_t readLockOrRestart(bool& needRestart) const {
    uint64_t version = version_lock_obsolete.load();
    if (isLocked(version) || isObsolete(version)) {
      needRestart = true;
    }
    return version;
  }


  void checkOrRestart(uint64_t startRead, bool& needRestart) const {
    readUnlockOrRestart(startRead, needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool& needRestart) const {
    needRestart = (startRead != version_lock_obsolete.load());
  }


  void writeLockOrRestart(bool& needRestart) {
    uint64_t version = readLockOrRestart(needRestart);
    if (needRestart) return;
    upgradeToWriteLockOrRestart(version, needRestart);
  }


  void upgradeToWriteLockOrRestart(uint64_t& version, bool& needRestart) {
    if (version_lock_obsolete.compare_exchange_strong(version, version + 0b10)) {
      version = version + 0b10;
    } else {
      needRestart = true;
    }
  }


  void writeUnlock() { version_lock_obsolete.fetch_add(0b10); }
};


constexpr int kLeafSlots = 32;
constexpr int kInnerSlots = 16;


struct LeafNode : public NodeHeader {
  const char* keys[kLeafSlots];
  LeafNode* next = nullptr;  
  LeafNode* prev = nullptr; 

  LeafNode() : NodeHeader(/*leaf=*/true) {}
  bool isFull() const { return count >= kLeafSlots; }
};

struct InnerNode : public NodeHeader {

  const char* separators[kInnerSlots];
  NodeHeader* children[kInnerSlots + 1];

  InnerNode() : NodeHeader(/*leaf=*/false) {}
  bool isFull() const { return count >= kInnerSlots; }
};


struct RootHolder : public NodeHeader {
  NodeHeader* child = nullptr;
  RootHolder() : NodeHeader(/*leaf=*/false) {}
};


struct KeyComparatorWrapper {
  const MemTableRep::KeyComparator* compare_;

  explicit KeyComparatorWrapper(const MemTableRep::KeyComparator* compare)
      : compare_(compare) {}

  bool operator()(const char* a, const char* b) const {
    return (*compare_)(a, b) < 0;
  }
};


class BTreeOLCTree {
 public:
  BTreeOLCTree(const KeyComparatorWrapper& cmp, Allocator* allocator)
      : cmp_(cmp), allocator_(allocator) {}

  ~BTreeOLCTree() = default;

  BTreeOLCTree(const BTreeOLCTree&) = delete;
  BTreeOLCTree& operator=(const BTreeOLCTree&) = delete;


  void insert(const char* key);

  bool contains(const char* key) const;


  void lookupRange(const char* start_key, void* cb_ctx,
                    bool (*callback)(void* arg, const char* entry)) const;


  struct Cursor {
    const LeafNode* leaf = nullptr;
    int slot = -1;            
    const char* key = nullptr;  
    bool valid = false;
  };

  void seek(const char* target, Cursor* cur) const;         
  void seekForPrev(const char* target, Cursor* cur) const;  
  void seekToFirst(Cursor* cur) const;
  void seekToLast(Cursor* cur) const;
  void next(Cursor* cur) const;
  void prev(Cursor* cur) const;

 private:
  RootHolder root_holder_;
  KeyComparatorWrapper cmp_;
  Allocator* allocator_;

  bool less(const char* a, const char* b) const { return cmp_(a, b); }
  bool equalKeys(const char* a, const char* b) const {
    return !less(a, b) && !less(b, a);
  }

  LeafNode* allocLeaf() const {
    void* mem = allocator_->AllocateAligned(sizeof(LeafNode));
    return new (mem) LeafNode();
  }
  InnerNode* allocInner() const {
    void* mem = allocator_->AllocateAligned(sizeof(InnerNode));
    return new (mem) InnerNode();
  }


  enum class FastInsertResult { kSuccess, kRetry, kFallBackToPessimistic };
  FastInsertResult insertOptimistic(const char* key);
  void insertPessimistic(const char* key);


  int childIndexFor(const InnerNode* node, const char* key) const {
    int i = 0;
    while (i < node->count && !less(key, node->separators[i])) i++;
    return i;
  }

 
  int leafLowerBound(const LeafNode* node, const char* key) const {
    int i = 0;
    while (i < node->count && less(node->keys[i], key)) i++;
    return i;
  }


  const LeafNode* findLeafOptimistic(const char* key) const;
  const LeafNode* leftmostLeafOptimistic() const;
  const LeafNode* rightmostLeafOptimistic() const;


  bool tryFastNext(Cursor* cur) const;
  bool tryFastPrev(Cursor* cur) const;
};

constexpr int kMaxTreeDepth = 64;  

bool isSafe(const NodeHeader* node) {
  return node->is_leaf ? !static_cast<const LeafNode*>(node)->isFull()
                       : !static_cast<const InnerNode*>(node)->isFull();
}


void splitInnerAndInsert(InnerNode* pnode, int idx, const char* promote_key,
                          NodeHeader* new_child, InnerNode* new_inner,
                          const char** out_promote) {
  const char* msep[kInnerSlots + 1];
  NodeHeader* mchild[kInnerSlots + 2];

  for (int i = 0; i < idx; i++) msep[i] = pnode->separators[i];
  msep[idx] = promote_key;
  for (int i = idx; i < pnode->count; i++) msep[i + 1] = pnode->separators[i];

  for (int i = 0; i <= idx; i++) mchild[i] = pnode->children[i];
  mchild[idx + 1] = new_child;
  for (int i = idx + 1; i <= pnode->count; i++) mchild[i + 1] = pnode->children[i];

  const int total_sep = pnode->count + 1;  
  const int mid = total_sep / 2;
  const int left_count = mid;
  const int right_count = total_sep - mid - 1;

  for (int i = 0; i < left_count; i++) pnode->separators[i] = msep[i];
  for (int i = 0; i <= left_count; i++) pnode->children[i] = mchild[i];
  pnode->count = left_count;

  for (int i = 0; i < right_count; i++) new_inner->separators[i] = msep[mid + 1 + i];
  for (int i = 0; i <= right_count; i++) new_inner->children[i] = mchild[mid + 1 + i];
  new_inner->count = right_count;

  *out_promote = msep[mid];
}

void BTreeOLCTree::insertPessimistic(const char* key) {
restart:
  bool needRestart = false;
  NodeHeader* stack[kMaxTreeDepth];
  int depth = 0;

  root_holder_.writeLockOrRestart(needRestart);
  if (needRestart) goto restart;
  stack[depth++] = &root_holder_;

  if (root_holder_.child == nullptr) {
    LeafNode* leaf = allocLeaf();
    leaf->keys[0] = key;
    leaf->count = 1;
    root_holder_.child = leaf; 
    root_holder_.writeUnlock();
    return;
  }

  {
    NodeHeader* node = root_holder_.child;
    node->writeLockOrRestart(needRestart);
    if (needRestart) {
      root_holder_.writeUnlock();
      goto restart;
    }
    if (isSafe(node)) {
      for (int i = 0; i < depth; i++) stack[i]->writeUnlock();
      depth = 0;
    }
    stack[depth++] = node;

    while (!node->is_leaf) {
      InnerNode* inner = static_cast<InnerNode*>(node);
      int idx = childIndexFor(inner, key);
      NodeHeader* child = inner->children[idx];
      child->writeLockOrRestart(needRestart);
      if (needRestart) {
        for (int i = 0; i < depth; i++) stack[i]->writeUnlock();
        goto restart;
      }
      if (isSafe(child)) {
        for (int i = 0; i < depth; i++) stack[i]->writeUnlock();
        depth = 0;
      }
      stack[depth++] = child;
      node = child;
    }


    LeafNode* leaf = static_cast<LeafNode*>(node);
    if (!leaf->isFull()) {
      int pos = leafLowerBound(leaf, key);
      for (int i = leaf->count; i > pos; i--) leaf->keys[i] = leaf->keys[i - 1];
      leaf->keys[pos] = key;
      leaf->count++;
      for (int i = 0; i < depth; i++) stack[i]->writeUnlock();
      return;
    }


    LeafNode* new_leaf = allocLeaf();
    int pos = leafLowerBound(leaf, key);
    const char* merged[kLeafSlots + 1];
    for (int i = 0; i < pos; i++) merged[i] = leaf->keys[i];
    merged[pos] = key;
    for (int i = pos; i < leaf->count; i++) merged[i + 1] = leaf->keys[i];
    const int total = leaf->count + 1;
    const int left_count = total / 2;
    const int right_count = total - left_count;
    for (int i = 0; i < left_count; i++) leaf->keys[i] = merged[i];
    leaf->count = left_count;
    for (int i = 0; i < right_count; i++) new_leaf->keys[i] = merged[left_count + i];
    new_leaf->count = right_count;
    const char* promote_key = new_leaf->keys[0];


    new_leaf->prev = leaf;
    new_leaf->next = leaf->next;
    if (leaf->next != nullptr) leaf->next->prev = new_leaf;
    leaf->next = new_leaf;

    leaf->writeUnlock();
    depth--;  

    NodeHeader* new_child = new_leaf;
    while (true) {
      NodeHeader* parent = stack[depth - 1];
      if (parent == &root_holder_) {
        InnerNode* new_root = allocInner();
        new_root->separators[0] = promote_key;
        new_root->children[0] = root_holder_.child;  
        new_root->children[1] = new_child;
        new_root->count = 1;
        root_holder_.child = new_root;  
        root_holder_.writeUnlock();
        return;
      }
      InnerNode* pnode = static_cast<InnerNode*>(parent);
      int idx = childIndexFor(pnode, promote_key);
      if (!pnode->isFull()) {
        for (int i = pnode->count; i > idx; i--) {
          pnode->separators[i] = pnode->separators[i - 1];
          pnode->children[i + 1] = pnode->children[i];
        }
        pnode->separators[idx] = promote_key;
        pnode->children[idx + 1] = new_child;
        pnode->count++;
        pnode->writeUnlock();
        return;
      }
      InnerNode* new_inner = allocInner();
      const char* new_promote;
      splitInnerAndInsert(pnode, idx, promote_key, new_child, new_inner, &new_promote);
      pnode->writeUnlock();
      promote_key = new_promote;
      new_child = new_inner;
      depth--;
    }
  }
}

constexpr int kMaxOptimisticInsertAttempts = 3;


BTreeOLCTree::FastInsertResult BTreeOLCTree::insertOptimistic(const char* key) {
  const LeafNode* leaf_const = findLeafOptimistic(key);
  if (leaf_const == nullptr) {
    return FastInsertResult::kFallBackToPessimistic; 
  }
  LeafNode* leaf = const_cast<LeafNode*>(leaf_const);

  bool needRestart = false;
  uint64_t v = leaf->readLockOrRestart(needRestart);
  if (needRestart) return FastInsertResult::kRetry;

  leaf->upgradeToWriteLockOrRestart(v, needRestart);
  if (needRestart) return FastInsertResult::kRetry;

  if (leaf->isFull()) {
    leaf->writeUnlock();
    return FastInsertResult::kFallBackToPessimistic; 
  }
  int pos = leafLowerBound(leaf, key);
  for (int i = leaf->count; i > pos; i--) leaf->keys[i] = leaf->keys[i - 1];
  leaf->keys[pos] = key;
  leaf->count++;
  leaf->writeUnlock();
  return FastInsertResult::kSuccess;
}

void BTreeOLCTree::insert(const char* key) {
  for (int attempt = 0; attempt < kMaxOptimisticInsertAttempts; attempt++) {
    FastInsertResult r = insertOptimistic(key);
    if (r == FastInsertResult::kSuccess) return;
    if (r == FastInsertResult::kFallBackToPessimistic) break;

  }
  insertPessimistic(key);
}

const LeafNode* BTreeOLCTree::findLeafOptimistic(const char* key) const {

  while (true) {
    bool needRestart = false;
    uint64_t pv = root_holder_.readLockOrRestart(needRestart);
    if (needRestart) continue;
    NodeHeader* node = root_holder_.child;
    root_holder_.checkOrRestart(pv, needRestart);
    if (needRestart) continue;
    if (node == nullptr) return nullptr;

    uint64_t nv = node->readLockOrRestart(needRestart);
    if (needRestart) continue;
    bool restart = false;
    while (!node->is_leaf) {
      const InnerNode* inner = static_cast<const InnerNode*>(node);
      int idx = childIndexFor(inner, key);
      NodeHeader* child = inner->children[idx];
      inner->checkOrRestart(nv, needRestart);
      if (needRestart) { restart = true; break; }
      uint64_t cv = child->readLockOrRestart(needRestart);
      if (needRestart) { restart = true; break; }
      node = child;
      nv = cv;
    }
    if (restart) continue;
    return static_cast<const LeafNode*>(node);
  }
}

bool BTreeOLCTree::contains(const char* key) const {
  while (true) {
    const LeafNode* leaf = findLeafOptimistic(key);
    if (leaf == nullptr) return false;
    bool needRestart = false;
    uint64_t version = leaf->readLockOrRestart(needRestart);
    if (needRestart) continue;
    int pos = leafLowerBound(leaf, key);
    bool found = (pos < leaf->count) && equalKeys(leaf->keys[pos], key);
    leaf->checkOrRestart(version, needRestart);
    if (needRestart) continue;
    return found;
  }
}

void BTreeOLCTree::lookupRange(const char* start_key, void* cb_ctx,
                               bool (*callback)(void* arg, const char* entry)) const {
  const char* resume_key = start_key;
  bool inclusive = true;

  while (true) {
    const LeafNode* leaf = findLeafOptimistic(resume_key);
    if (leaf == nullptr) return;
    bool needRestart = false;
    uint64_t version = leaf->readLockOrRestart(needRestart);
    if (needRestart) continue;

    int pos = leafLowerBound(leaf, resume_key);
    if (!inclusive && pos < leaf->count && equalKeys(leaf->keys[pos], resume_key)) {
      pos++;
    }

    const char* last_key = nullptr;
    bool need_full_restart = false;
    while (true) {
      if (pos >= leaf->count) {
        const LeafNode* nxt = leaf->next;
        bool nr = false;
        leaf->checkOrRestart(version, nr);
        if (nr) { need_full_restart = true; break; }
        if (nxt == nullptr) return;
        uint64_t nv = nxt->readLockOrRestart(nr);
        if (nr) { need_full_restart = true; break; }
        leaf = nxt;
        version = nv;
        pos = 0;
        continue;
      }
      const char* k = leaf->keys[pos];
      bool nr = false;
      leaf->checkOrRestart(version, nr);
      if (nr) { need_full_restart = true; break; }
      last_key = k;
      if (!callback(cb_ctx, k)) return;
      pos++;
    }
    if (!need_full_restart) return;
    if (last_key != nullptr) {
      resume_key = last_key;
      inclusive = false;
    }
  }
}


void BTreeOLCTree::seek(const char* target, Cursor* cur) const {
  while (true) {
    const LeafNode* leaf = findLeafOptimistic(target);
    if (leaf == nullptr) { cur->valid = false; return; }
    bool needRestart = false;
    uint64_t version = leaf->readLockOrRestart(needRestart);
    if (needRestart) continue;

    int pos = leafLowerBound(leaf, target);
    if (pos < leaf->count) {
      const char* k = leaf->keys[pos];
      leaf->checkOrRestart(version, needRestart);
      if (needRestart) continue;
      cur->leaf = leaf; cur->slot = pos; cur->key = k; cur->valid = true;
      return;
    }

    const LeafNode* nxt = leaf->next;
    leaf->checkOrRestart(version, needRestart);
    if (needRestart) continue;
    if (nxt == nullptr) { cur->valid = false; return; }
    uint64_t nv = nxt->readLockOrRestart(needRestart);
    if (needRestart) continue;
    if (nxt->count == 0) continue;  
    const char* k = nxt->keys[0];
    nxt->checkOrRestart(nv, needRestart);
    if (needRestart) continue;
    cur->leaf = nxt; cur->slot = 0; cur->key = k; cur->valid = true;
    return;
  }
}

void BTreeOLCTree::seekForPrev(const char* target, Cursor* cur) const {
  while (true) {
    const LeafNode* leaf = findLeafOptimistic(target);
    if (leaf == nullptr) { cur->valid = false; return; }
    bool needRestart = false;
    uint64_t version = leaf->readLockOrRestart(needRestart);
    if (needRestart) continue;


    int pos = leafLowerBound(leaf, target);
    if (pos < leaf->count && equalKeys(leaf->keys[pos], target)) pos++;
    if (pos > 0) {
      const char* k = leaf->keys[pos - 1];
      leaf->checkOrRestart(version, needRestart);
      if (needRestart) continue;
      cur->leaf = leaf; cur->slot = pos - 1; cur->key = k; cur->valid = true;
      return;
    }
    const LeafNode* prv = leaf->prev;
    leaf->checkOrRestart(version, needRestart);
    if (needRestart) continue;
    if (prv == nullptr) { cur->valid = false; return; }
    uint64_t pv = prv->readLockOrRestart(needRestart);
    if (needRestart) continue;
    if (prv->count == 0) continue;
    const char* k = prv->keys[prv->count - 1];
    prv->checkOrRestart(pv, needRestart);
    if (needRestart) continue;
    cur->leaf = prv; cur->slot = prv->count - 1; cur->key = k; cur->valid = true;
    return;
  }
}

const LeafNode* BTreeOLCTree::leftmostLeafOptimistic() const {
  while (true) {
    bool needRestart = false;
    uint64_t pv = root_holder_.readLockOrRestart(needRestart);
    if (needRestart) continue;
    NodeHeader* node = root_holder_.child;
    root_holder_.checkOrRestart(pv, needRestart);
    if (needRestart) continue;
    if (node == nullptr) return nullptr;

    uint64_t nv = node->readLockOrRestart(needRestart);
    if (needRestart) continue;
    bool restart = false;
    while (!node->is_leaf) {
      const InnerNode* inner = static_cast<const InnerNode*>(node);
      NodeHeader* child = inner->children[0];
      inner->checkOrRestart(nv, needRestart);
      if (needRestart) { restart = true; break; }
      uint64_t cv = child->readLockOrRestart(needRestart);
      if (needRestart) { restart = true; break; }
      node = child;
      nv = cv;
    }
    if (restart) continue;
    return static_cast<const LeafNode*>(node);
  }
}

const LeafNode* BTreeOLCTree::rightmostLeafOptimistic() const {
  while (true) {
    bool needRestart = false;
    uint64_t pv = root_holder_.readLockOrRestart(needRestart);
    if (needRestart) continue;
    NodeHeader* node = root_holder_.child;
    root_holder_.checkOrRestart(pv, needRestart);
    if (needRestart) continue;
    if (node == nullptr) return nullptr;

    uint64_t nv = node->readLockOrRestart(needRestart);
    if (needRestart) continue;
    bool restart = false;
    while (!node->is_leaf) {
      const InnerNode* inner = static_cast<const InnerNode*>(node);
      NodeHeader* child = inner->children[inner->count];
      inner->checkOrRestart(nv, needRestart);
      if (needRestart) { restart = true; break; }
      uint64_t cv = child->readLockOrRestart(needRestart);
      if (needRestart) { restart = true; break; }
      node = child;
      nv = cv;
    }
    if (restart) continue;
    return static_cast<const LeafNode*>(node);
  }
}

void BTreeOLCTree::seekToFirst(Cursor* cur) const {
  while (true) {
    const LeafNode* leaf = leftmostLeafOptimistic();
    if (leaf == nullptr) { cur->valid = false; return; }
    bool needRestart = false;
    uint64_t version = leaf->readLockOrRestart(needRestart);
    if (needRestart) continue;
    if (leaf->count == 0) continue; 
    const char* k = leaf->keys[0];
    leaf->checkOrRestart(version, needRestart);
    if (needRestart) continue;
    cur->leaf = leaf; cur->slot = 0; cur->key = k; cur->valid = true;
    return;
  }
}

void BTreeOLCTree::seekToLast(Cursor* cur) const {
  while (true) {
    const LeafNode* leaf = rightmostLeafOptimistic();
    if (leaf == nullptr) { cur->valid = false; return; }
    bool needRestart = false;
    uint64_t version = leaf->readLockOrRestart(needRestart);
    if (needRestart) continue;
    if (leaf->count == 0) continue;
    const char* k = leaf->keys[leaf->count - 1];
    leaf->checkOrRestart(version, needRestart);
    if (needRestart) continue;
    cur->leaf = leaf; cur->slot = leaf->count - 1; cur->key = k; cur->valid = true;
    return;
  }
}

bool BTreeOLCTree::tryFastNext(Cursor* cur) const {
  const LeafNode* leaf = cur->leaf;
  bool needRestart = false;
  uint64_t version = leaf->readLockOrRestart(needRestart);
  if (needRestart) return false;

  int next_slot = cur->slot + 1;
  if (next_slot < leaf->count) {
    const char* k = leaf->keys[next_slot];
    leaf->checkOrRestart(version, needRestart);
    if (needRestart) return false;
    cur->slot = next_slot;
    cur->key = k;
    return true;
  }
  const LeafNode* nxt = leaf->next;
  leaf->checkOrRestart(version, needRestart);
  if (needRestart) return false;
  if (nxt == nullptr) {
    cur->valid = false;
    return true;  
  }
  uint64_t nv = nxt->readLockOrRestart(needRestart);
  if (needRestart) return false;
  if (nxt->count == 0) return false;  
  const char* k = nxt->keys[0];
  nxt->checkOrRestart(nv, needRestart);
  if (needRestart) return false;
  cur->leaf = nxt;
  cur->slot = 0;
  cur->key = k;
  return true;
}

bool BTreeOLCTree::tryFastPrev(Cursor* cur) const {
  const LeafNode* leaf = cur->leaf;
  bool needRestart = false;
  uint64_t version = leaf->readLockOrRestart(needRestart);
  if (needRestart) return false;

  if (cur->slot > 0) {
    int p = cur->slot - 1;
    const char* k = leaf->keys[p];
    leaf->checkOrRestart(version, needRestart);
    if (needRestart) return false;
    cur->slot = p;
    cur->key = k;
    return true;
  }
  const LeafNode* prv = leaf->prev;
  leaf->checkOrRestart(version, needRestart);
  if (needRestart) return false;
  if (prv == nullptr) {
    cur->valid = false;
    return true;
  }
  uint64_t pv = prv->readLockOrRestart(needRestart);
  if (needRestart) return false;
  if (prv->count == 0) return false;
  const char* k = prv->keys[prv->count - 1];
  prv->checkOrRestart(pv, needRestart);
  if (needRestart) return false;
  cur->leaf = prv;
  cur->slot = prv->count - 1;
  cur->key = k;
  return true;
}

void BTreeOLCTree::next(Cursor* cur) const {
  if (!cur->valid) return;
  if (tryFastNext(cur)) return;

  const char* cur_key = cur->key;
  while (true) {
    seek(cur_key, cur);
    if (!cur->valid || !equalKeys(cur->key, cur_key)) return;
    if (tryFastNext(cur)) return;
  }
}

void BTreeOLCTree::prev(Cursor* cur) const {
  if (!cur->valid) return;
  if (tryFastPrev(cur)) return;
  const char* cur_key = cur->key;
  while (true) {
    seekForPrev(cur_key, cur);
    if (!cur->valid || !equalKeys(cur->key, cur_key)) return;
    if (tryFastPrev(cur)) return;
  }
}

}  // namespace



class OLCBTreeRep : public MemTableRep {
 public:
  explicit OLCBTreeRep(const MemTableRep::KeyComparator& cmp, Allocator* allocator)
      : MemTableRep(allocator),
        cmp_(cmp),
        allocator_(allocator),
        tree_(KeyComparatorWrapper(&cmp_), allocator) {}

  virtual ~OLCBTreeRep() override {}

  virtual KeyHandle Allocate(const size_t len, char** buf) override {
    *buf = allocator_->Allocate(len);
    return static_cast<KeyHandle>(*buf);
  }

  virtual void Insert(KeyHandle handle) override {
    const char* key = static_cast<const char*>(handle);
    tree_.insert(key);
  }

  virtual void InsertWithHint(KeyHandle handle, void** hint) override {
    Insert(handle);
  }

  virtual void InsertConcurrently(KeyHandle handle) override { Insert(handle); }

  virtual void InsertWithHintConcurrently(KeyHandle handle, void** hint) override {
    InsertConcurrently(handle);
  }

  virtual bool Contains(const char* key) const override { return tree_.contains(key); }

  virtual size_t ApproximateMemoryUsage() override { return 0; }

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg, const char* entry)) override {
    const char* target = k.memtable_key().data();
    tree_.lookupRange(target, callback_args, callback_func);
  }

  virtual MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  virtual MemTableRep::Iterator* GetDynamicPrefixIterator(
      Arena* arena = nullptr) override {
    return GetIterator(arena);
  }

 private:
  friend class OLCBTreeIterator;
  const MemTableRep::KeyComparator& cmp_;
  Allocator* const allocator_;
  BTreeOLCTree tree_;
};

class OLCBTreeIterator : public MemTableRep::Iterator {
 public:
  explicit OLCBTreeIterator(OLCBTreeRep* rep) : rep_(rep) {}

  virtual ~OLCBTreeIterator() override {}

  virtual bool Valid() const override { return cursor_.valid; }

  virtual const char* key() const override {
    assert(Valid());
    return cursor_.key;
  }

  virtual void Next() override {
    assert(Valid());
    rep_->tree_.next(&cursor_);
  }

  virtual void Prev() override {
    assert(Valid());
    rep_->tree_.prev(&cursor_);
  }

  virtual void Seek(const Slice& user_key, const char* memtable_key) override {
    const char* encoded_key =
        (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
    rep_->tree_.seek(encoded_key, &cursor_);
  }

  virtual void SeekForPrev(const Slice& user_key,
                           const char* memtable_key) override {
    const char* encoded_key =
        (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
    rep_->tree_.seekForPrev(encoded_key, &cursor_);
  }

  virtual void SeekToFirst() override { rep_->tree_.seekToFirst(&cursor_); }

  virtual void SeekToLast() override { rep_->tree_.seekToLast(&cursor_); }

 private:
  OLCBTreeRep* rep_;
  BTreeOLCTree::Cursor cursor_;
  std::string tmp_;
};

MemTableRep::Iterator* OLCBTreeRep::GetIterator(Arena* arena) {
  if (arena != nullptr) {
    void* mem = arena->AllocateAligned(sizeof(OLCBTreeIterator));
    return new (mem) OLCBTreeIterator(this);
  } else {
    return new OLCBTreeIterator(this);
  }
}

class OLCBTreeRepFactory : public MemTableRepFactory {
 public:
  explicit OLCBTreeRepFactory() {}

  virtual ~OLCBTreeRepFactory() {}

  using MemTableRepFactory::CreateMemTableRep;

  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& cmp, Allocator* allocator,
      const SliceTransform* transform, Logger* logger) override {
    return new OLCBTreeRep(cmp, allocator);
  }

  virtual const char* Name() const override { return "OLCBTreeRepFactory"; }


  virtual bool IsInsertConcurrentlySupported() const override { return true; }
};

MemTableRepFactory* NewOLCBTreeRepFactory() {
  return new OLCBTreeRepFactory();
}

}  // namespace rocksdb

#endif 
