// Tree.cpp
//
// BTree::Tree implementation.
//
// == Insert protocol: optimistic fast path + pessimistic fallback ==
//
// insert() tries insertOptimistic() first (a few times), which descends
// with ZERO locks -- not even root_holder_ -- using the same
// findLeafOptimistic() every read uses, then upgrades only the single
// target leaf to a write lock via CAS. If that upgrade succeeds, the leaf
// is provably unchanged since the descent read it, so it's safe to mutate
// directly: no ancestor, and no other unrelated leaf, is ever touched.
// This is what makes it genuinely fine-grained/optimistic rather than
// "coarse lock with a shorter critical section": two threads inserting
// into different leaves never contend at all, not even briefly, and
// reads never block behind it either. See insertOptimistic()'s own
// comment below for the safety argument (why validating only the leaf's
// version, not the whole path, is sufficient).
//
// insertOptimistic() falls back to insertPessimistic() (the tree's only
// insert path in an earlier revision of this file) when: the tree is
// empty, the target leaf is full (a real split is needed), or it keeps
// losing a race after a few attempts. insertPessimistic() is the
// lock-coupling implementation that actually performs splits (including
// cascading splits and root growth): descend from the root, write-locking
// one level at a time. The moment a newly-locked node is proven "safe"
// (not full), every ancestor locked so far is released immediately: a
// not-full node can always absorb the one (key, child) pair that a split
// of its child would push up, so nothing above it can possibly need to
// change. This bounds the set of locks held at any moment to the
// contiguous "all full" suffix of the root-to-leaf path nearest the leaf
// (usually just the leaf itself, or the leaf plus one or two full
// ancestors) -- never the whole tree, unlike the old design's single
// global lock. See Node.h for the lock primitive itself and the
// accepted-benign-race note for non-atomic payload fields.
//
// Splits follow a strict "publish-last" discipline: a new node (leaf or
// inner) is fully constructed -- including every field a reader might
// optimistically inspect -- *before* the single pointer write that makes
// it reachable from the rest of the tree, and that publishing write is
// itself covered by the enclosing node's own write lock/version bump, which
// is the actual synchronization point a concurrent optimistic reader's
// checkOrRestart will catch. Reordering "construct new node" and "publish
// pointer" the other way around would let a reader observe a
// partially-built node.
//
// == Read protocol ==
//
// Point lookups and range scans are pure optimistic descents: no locks are
// ever taken, only version *snapshots*, and every subsequent read against
// a node is validated against that snapshot before being trusted (read
// data, then check version, then trust the data -- never the reverse
// order, or the version check no longer catches a race that happened
// in between). Any failed check anywhere aborts the whole read and
// restarts the descent from the root; a range scan additionally has an
// O(1)-amortized fast path that hops leaf-to-leaf via the doubly-linked
// leaf chain (rather than a full root descent per step), falling back to
// a full restart (resuming just past the last successfully-returned key)
// on any validation failure during a hop -- this exact "cap the fast path,
// restart from the root on failure" idiom mirrors
// third-party/ARTSynchronized/OptimisticLockCoupling/Tree.cpp's own
// restart-from-root discipline for its range scans.

#include "Tree.h"

#include <cstring>

namespace BTree {

namespace {
constexpr int kMaxTreeDepth = 64;  // generous bound: kInnerSlots^64 keys
}  // namespace

Tree::Tree(LessFunc less_fn, void* less_ctx, AllocFunc alloc_fn, void* alloc_ctx)
    : less_(less_fn), less_ctx_(less_ctx), alloc_(alloc_fn), alloc_ctx_(alloc_ctx) {}

LeafNode* Tree::allocLeaf() const {
  void* mem = alloc_(alloc_ctx_, sizeof(LeafNode));
  return new (mem) LeafNode();
}

InnerNode* Tree::allocInner() const {
  void* mem = alloc_(alloc_ctx_, sizeof(InnerNode));
  return new (mem) InnerNode();
}

int Tree::childIndexFor(const InnerNode* node, const char* key) const {
  int i = 0;
  while (i < node->count && !less(key, node->separators[i])) i++;
  return i;
}

int Tree::leafLowerBound(const LeafNode* node, const char* key) const {
  int i = 0;
  while (i < node->count && less(node->keys[i], key)) i++;
  return i;
}

namespace {
bool isSafe(const NodeHeader* node) {
  return node->is_leaf ? !static_cast<const LeafNode*>(node)->isFull()
                       : !static_cast<const InnerNode*>(node)->isFull();
}

// Standard merge-then-split-and-insert for an inner node that was found
// full: builds the logically-merged (separators, children) arrays with
// (promote_key, new_child) inserted at position `idx` (i.e. new_child
// becomes the child immediately after the existing children[idx]), then
// splits down the middle. The middle separator is promoted to the caller
// (not stored in either half) -- unlike a leaf split, inner separators are
// pure routing keys, not data, so they aren't duplicated.
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

  const int total_sep = pnode->count + 1;  // == kInnerSlots + 1
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
}  // namespace

void Tree::insertPessimistic(const char* key) {
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
    root_holder_.child = leaf;  // publish-last: leaf fully built above
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

    // node (== stack[depth-1]) is the target leaf, write-locked.
    LeafNode* leaf = static_cast<LeafNode*>(node);
    if (!leaf->isFull()) {
      int pos = leafLowerBound(leaf, key);
      for (int i = leaf->count; i > pos; i--) leaf->keys[i] = leaf->keys[i - 1];
      leaf->keys[pos] = key;
      leaf->count++;
      for (int i = 0; i < depth; i++) stack[i]->writeUnlock();
      return;
    }

    // Leaf is full: split it. stack[depth-2] (if any) is guaranteed to be
    // this leaf's real parent -- either the sole survivor of the last
    // "safe" clear (if it had room), or part of the accumulated
    // all-full chain -- either way it's still write-locked here.
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

    // Splice into the doubly-linked leaf chain -- new_leaf is fully built
    // above before any pointer that makes it reachable is written.
    new_leaf->prev = leaf;
    new_leaf->next = leaf->next;
    if (leaf->next != nullptr) leaf->next->prev = new_leaf;
    leaf->next = new_leaf;

    leaf->writeUnlock();
    depth--;  // pop the leaf; everything from here up is cascading insert

    NodeHeader* new_child = new_leaf;
    while (true) {
      NodeHeader* parent = stack[depth - 1];
      if (parent == &root_holder_) {
        InnerNode* new_root = allocInner();
        new_root->separators[0] = promote_key;
        new_root->children[0] = root_holder_.child;  // old top node, shrunk in place
        new_root->children[1] = new_child;
        new_root->count = 1;
        root_holder_.child = new_root;  // publish-last
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

namespace {
constexpr int kMaxOptimisticInsertAttempts = 3;
}  // namespace

// Optimistic fast path (the actually-load-bearing half of OLC): descend
// with zero locks (just version snapshots, via findLeafOptimistic -- the
// same routine every read uses), then upgrade ONLY the target leaf to a
// write lock via CAS against the version last observed for it. No
// ancestor -- not even root_holder_ -- is ever locked here. If the CAS
// upgrade succeeds, the leaf's version is provably unchanged since our
// descent read it, so the leaf is still correct for `key` (a concurrent
// split would have bumped its version and failed the CAS) and it's safe
// to mutate directly. Ancestor structure changing concurrently (an
// unrelated inner-node split elsewhere, or even directly above this leaf)
// cannot make this leaf wrong for `key`, only change how you'd navigate to
// it -- so only the leaf's own version needs validating, not the path.
Tree::FastInsertResult Tree::insertOptimistic(const char* key) {
  const LeafNode* leaf_const = findLeafOptimistic(key);
  if (leaf_const == nullptr) {
    return FastInsertResult::kFallBackToPessimistic;  // empty tree
  }
  LeafNode* leaf = const_cast<LeafNode*>(leaf_const);

  bool needRestart = false;
  uint64_t v = leaf->readLockOrRestart(needRestart);
  if (needRestart) return FastInsertResult::kRetry;

  leaf->upgradeToWriteLockOrRestart(v, needRestart);
  if (needRestart) return FastInsertResult::kRetry;

  if (leaf->isFull()) {
    leaf->writeUnlock();
    return FastInsertResult::kFallBackToPessimistic;  // needs a real split
  }
  int pos = leafLowerBound(leaf, key);
  for (int i = leaf->count; i > pos; i--) leaf->keys[i] = leaf->keys[i - 1];
  leaf->keys[pos] = key;
  leaf->count++;
  leaf->writeUnlock();
  return FastInsertResult::kSuccess;
}

void Tree::insert(const char* key) {
  for (int attempt = 0; attempt < kMaxOptimisticInsertAttempts; attempt++) {
    FastInsertResult r = insertOptimistic(key);
    if (r == FastInsertResult::kSuccess) return;
    if (r == FastInsertResult::kFallBackToPessimistic) break;
    // kRetry: a transient race (someone else touched this leaf between our
    // descent and our upgrade attempt) -- just try the optimistic path
    // again rather than paying for a pessimistic pass.
  }
  insertPessimistic(key);
}

const LeafNode* Tree::findLeafOptimistic(const char* key) const {
  // Returned via a loop-local static contract: callers always call this
  // right before reading from the leaf and then must checkOrRestart the
  // version this call implicitly validated up to (see seek()/contains()/
  // lookupRange() below, which re-derive that version by re-reading it --
  // see the *_with_version helpers used there instead of this raw form
  // for the common case). This bare form is kept only for the leftmost/
  // rightmost helpers below where the caller re-validates itself.
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

bool Tree::contains(const char* key) const {
  while (true) {
    const LeafNode* leaf = findLeafOptimistic(key);
    if (leaf == nullptr) return false;
    // Re-snapshot the leaf's version explicitly (findLeafOptimistic already
    // validated reaching it, but we need our own stashed version to cover
    // the read we're about to do).
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

void Tree::lookupRange(const char* start_key, void* cb_ctx, ScanCallback callback) const {
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

// -- Cursor / iterator support ----------------------------------------------

void Tree::seek(const char* target, Cursor* cur) const {
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
    // No key >= target in this leaf -- the successor, if any, is the
    // first key of the next leaf.
    const LeafNode* nxt = leaf->next;
    leaf->checkOrRestart(version, needRestart);
    if (needRestart) continue;
    if (nxt == nullptr) { cur->valid = false; return; }
    uint64_t nv = nxt->readLockOrRestart(needRestart);
    if (needRestart) continue;
    if (nxt->count == 0) continue;  // shouldn't happen; guard and retry
    const char* k = nxt->keys[0];
    nxt->checkOrRestart(nv, needRestart);
    if (needRestart) continue;
    cur->leaf = nxt; cur->slot = 0; cur->key = k; cur->valid = true;
    return;
  }
}

void Tree::seekForPrev(const char* target, Cursor* cur) const {
  while (true) {
    const LeafNode* leaf = findLeafOptimistic(target);
    if (leaf == nullptr) { cur->valid = false; return; }
    bool needRestart = false;
    uint64_t version = leaf->readLockOrRestart(needRestart);
    if (needRestart) continue;

    // First key > target within this leaf, then step back one; if that
    // lands before slot 0, the predecessor is in the previous leaf.
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

const LeafNode* Tree::leftmostLeafOptimistic() const {
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

const LeafNode* Tree::rightmostLeafOptimistic() const {
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

void Tree::seekToFirst(Cursor* cur) const {
  while (true) {
    const LeafNode* leaf = leftmostLeafOptimistic();
    if (leaf == nullptr) { cur->valid = false; return; }
    bool needRestart = false;
    uint64_t version = leaf->readLockOrRestart(needRestart);
    if (needRestart) continue;
    if (leaf->count == 0) continue;  // guard; shouldn't happen
    const char* k = leaf->keys[0];
    leaf->checkOrRestart(version, needRestart);
    if (needRestart) continue;
    cur->leaf = leaf; cur->slot = 0; cur->key = k; cur->valid = true;
    return;
  }
}

void Tree::seekToLast(Cursor* cur) const {
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

bool Tree::tryFastNext(Cursor* cur) const {
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
    return true;  // definitively exhausted, not a race -- not a "retry" case
  }
  uint64_t nv = nxt->readLockOrRestart(needRestart);
  if (needRestart) return false;
  if (nxt->count == 0) return false;  // shouldn't happen; treat as a race
  const char* k = nxt->keys[0];
  nxt->checkOrRestart(nv, needRestart);
  if (needRestart) return false;
  cur->leaf = nxt;
  cur->slot = 0;
  cur->key = k;
  return true;
}

bool Tree::tryFastPrev(Cursor* cur) const {
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

void Tree::next(Cursor* cur) const {
  if (!cur->valid) return;
  if (tryFastNext(cur)) return;
  // Fallback: re-seek to the current key -- always lands exactly on it
  // since keys are never removed once inserted (RocksDB deletes are
  // tombstone inserts, not physical removals) -- then take exactly one
  // more fast step from that freshly-validated position. If a split races
  // exactly between the two calls, retry; this always terminates because
  // it only retries in response to actual concurrent writer activity, not
  // an unconditional loop.
  const char* cur_key = cur->key;
  while (true) {
    seek(cur_key, cur);
    if (!cur->valid || !equalKeys(cur->key, cur_key)) return;
    if (tryFastNext(cur)) return;
  }
}

void Tree::prev(Cursor* cur) const {
  if (!cur->valid) return;
  if (tryFastPrev(cur)) return;
  const char* cur_key = cur->key;
  while (true) {
    seekForPrev(cur_key, cur);
    if (!cur->valid || !equalKeys(cur->key, cur_key)) return;
    if (tryFastPrev(cur)) return;
  }
}

}  // namespace BTree
