// Tree.h
//
// BTree::Tree -- a concurrent B+Tree using Optimistic Lock Coupling
// (OLC), replacing the previous global-rwlock TLX B+Tree memtable
// implementation. See Node.h for the node layout and lock primitive, and
// the file header of Tree.cpp for the insert/split and read protocols.
//
// This library is intentionally independent of RocksDB (mirroring
// third-party/ARTSynchronized/OptimisticLockCoupling's own independence): the key
// comparator and node allocator are both supplied as plain function
// pointer + opaque context pairs (the same runtime-callback style ART_OLC
// uses for its LoadKeyFunction), not templates or virtual interfaces, so
// this stays a single ordinary (non-template) translation unit.

#ifndef BTREE_OLC_TREE_H
#define BTREE_OLC_TREE_H

#include <cstddef>
#include "Node.h"

namespace BTree {

// Returns true if a < b (strict weak ordering). Keys are opaque
// caller-owned byte strings (e.g. RocksDB internal-key-encoded buffers);
// this tree never inspects their bytes itself, only via this callback.
using LessFunc = bool (*)(void* ctx, const char* a, const char* b);

// Allocates `size` bytes for one node and returns a pointer suitable for
// placement-new. Nodes are never individually freed (see Node.h's comment
// on why no reclamation is needed) -- the caller's allocator (e.g. a
// RocksDB Arena) reclaims everything in bulk when the tree itself is torn
// down, so this tree has no Free/Deallocate counterpart at all.
using AllocFunc = void* (*)(void* ctx, size_t size);

// Generic key/value scan callback: return false to stop the scan early.
using ScanCallback = bool (*)(void* cb_ctx, const char* key);

class Tree {
 public:
  Tree(LessFunc less_fn, void* less_ctx, AllocFunc alloc_fn, void* alloc_ctx);

  // No reclamation is ever needed (Node.h), so there is nothing for this
  // destructor to do -- the underlying arena outlives (and reclaims) every
  // node this tree ever allocated.
  ~Tree() = default;

  Tree(const Tree&) = delete;
  Tree& operator=(const Tree&) = delete;

  // Inserts `key`. Safe to call from multiple threads concurrently with
  // each other and with lookups/scans (this is the whole point).
  void insert(const char* key);

  // Returns true iff a key equal to `key` (neither less(a,b) nor less(b,a))
  // is present.
  bool contains(const char* key) const;

  // Scans keys in ascending order starting from the first key >= start_key,
  // calling callback(cb_ctx, key) for each, until callback returns false or
  // the tree is exhausted.
  void lookupRange(const char* start_key, void* cb_ctx,
                    ScanCallback callback) const;

  // -- Iterator support --------------------------------------------------
  // A Cursor identifies a position (a specific key within a specific leaf)
  // for MemTableRep::Iterator-style forward/backward stepping. Seek*
  // methods always produce a correct cursor via a fresh root descent.
  // Next()/Prev() try an O(1) leaf-chain hop first, validated by version
  // check, and fall back to a fresh Seek-by-key on any validation failure
  // -- so they are always correct, just not always O(1).
  struct Cursor {
    const LeafNode* leaf = nullptr;
    int slot = -1;          // index into leaf->keys[]
    const char* key = nullptr;  // leaf->keys[slot], cached for convenience
    bool valid = false;
  };

  void seek(const char* target, Cursor* cur) const;         // first key >= target
  void seekForPrev(const char* target, Cursor* cur) const;  // last key <= target
  void seekToFirst(Cursor* cur) const;
  void seekToLast(Cursor* cur) const;
  void next(Cursor* cur) const;
  void prev(Cursor* cur) const;

 private:
  RootHolder root_holder_;
  LessFunc less_;
  void* less_ctx_;
  AllocFunc alloc_;
  void* alloc_ctx_;

  bool less(const char* a, const char* b) const { return less_(less_ctx_, a, b); }
  bool equalKeys(const char* a, const char* b) const {
    return !less(a, b) && !less(b, a);
  }

  LeafNode* allocLeaf() const;
  InnerNode* allocInner() const;

  // insert()'s two-phase implementation: try the lock-free-descent,
  // single-leaf-CAS-upgrade fast path a few times (insertOptimistic), and
  // only fall back to the full pessimistic lock-coupling pass
  // (insertPessimistic, handles splits/empty-tree) when the fast path
  // can't make progress. See Tree.cpp's file header and the comment above
  // insertOptimistic's definition for the full protocol/safety argument.
  enum class FastInsertResult { kSuccess, kRetry, kFallBackToPessimistic };
  FastInsertResult insertOptimistic(const char* key);
  void insertPessimistic(const char* key);

  // Index of the child of `node` that key belongs under (upper_bound over
  // separators).
  int childIndexFor(const InnerNode* node, const char* key) const;

  // Index of the first slot in `node` with keys[slot] >= key (lower_bound);
  // may equal node->count if all keys are smaller.
  int leafLowerBound(const LeafNode* node, const char* key) const;

  // Optimistic (lock-free) descent from the root to the leaf that would
  // contain `key`, retried from scratch on any validation failure. Never
  // fails to return a plausible leaf; the *caller* re-validates whatever it
  // reads from that leaf against a stashed version, same as every other
  // optimistic read in this tree.
  const LeafNode* findLeafOptimistic(const char* key) const;
  const LeafNode* leftmostLeafOptimistic() const;
  const LeafNode* rightmostLeafOptimistic() const;

  // Attempts the O(1) leaf-local/leaf-chain-hop step from `cur`'s current
  // position, validated by version check. Returns false (cursor left
  // unmodified in its pre-call, still-valid state at cur_key) if any
  // check failed, in which case the caller falls back to a full by-key
  // re-seek. `cur` must already be valid on entry.
  bool tryFastNext(Cursor* cur) const;
  bool tryFastPrev(Cursor* cur) const;
};

}  // namespace BTree

#endif  // BTREE_OLC_TREE_H
