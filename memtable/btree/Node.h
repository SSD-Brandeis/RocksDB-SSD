// Node.h
//
// Node layout for BTree, a fine-grained Optimistic Lock Coupling (OLC)
// B+Tree replacing the previous global-rwlock-guarded TLX B+Tree memtable.
//
// This mirrors the version/lock primitive already used and TSAN-validated
// in this repo's ART implementation
// (third-party/ARTSynchronized/OptimisticLockCoupling/N.h, N.cpp) so both
// concurrent-tree memtables in this project share one well-established
// synchronization technique.

#ifndef BTREE_NODE_H
#define BTREE_NODE_H

#include <atomic>
#include <cstdint>
#include <cstddef>

namespace BTree {

// Optimistic Lock Coupling version/lock word.
//
// Layout: bit0 = obsolete, bit1 = lock, bits 2-63 = version counter. This
// tree never sets the obsolete bit -- unlike ART's grow-in-place (which
// retires and reclaims the old node on every structural change), a real
// B+Tree *split* keeps both halves live and reachable forever: the old
// node becomes one half of the split, a new node is allocated for the
// other half, and even a root split just wraps a new root around the old
// one as a child. So no node ever becomes unreachable during normal
// operation, and no reclamation (à la ART's Epoche) is needed at all. The
// obsolete bit is kept in the layout only for structural parity with the
// ART lock word and is always 0.
//
// IMPORTANT -- benign-race-by-design, documented once here rather than
// left as tribal knowledge (as it is in the upstream ART_OLC headers this
// mirrors): only this version_lock_obsolete_ word is ever accessed
// atomically. Every other field on LeafNode/InnerNode below (keys,
// separators, children, count) is PLAIN, non-atomic memory. Writers
// mutate these fields with ordinary stores/memmove while holding this
// node's write lock; optimistic readers read them with ordinary loads and
// no atomics or fences at all. This is a deliberate, well-established OLC
// pattern: an optimistic reader can race a concurrent writer and observe a
// torn or partially-updated field, but readLockOrRestart/checkOrRestart
// detects any such race after the fact (the version word will have
// changed) and discards the read, forcing a retry -- so the race can
// never be *observed* as a wrong answer, only as a wasted, retried
// attempt. This is a real C++ data race in the formal sense, accepted as
// benign by construction; it is the exact same class of race this
// project's ART_OLC port already produces under TSAN and treats as
// "by design, same as upstream" rather than a bug (see project memory
// memtable-concurrency-revision.md). The one discipline that must be
// followed everywhere to keep this sound: read data, THEN check the
// version, THEN trust the data -- never the other order.
class NodeHeader {
 public:
  std::atomic<uint64_t> version_lock_obsolete{0b00};
  uint16_t count = 0;
  const bool is_leaf;

  explicit NodeHeader(bool leaf) : is_leaf(leaf) {}

  static bool isLocked(uint64_t version) { return (version & 0b10) == 0b10; }
  static bool isObsolete(uint64_t version) { return (version & 1) == 1; }

  // == third-party/ARTSynchronized/OptimisticLockCoupling/N.cpp:267-280 ==
  uint64_t readLockOrRestart(bool& needRestart) const {
    uint64_t version = version_lock_obsolete.load();
    if (isLocked(version) || isObsolete(version)) {
      needRestart = true;
    }
    return version;
  }

  // == N.cpp:286-292 == (checkOrRestart is just readUnlockOrRestart under
  // another name upstream too -- kept as a separate method for readability
  // at call sites that are "validating a stashed version" vs. "unlocking".)
  void checkOrRestart(uint64_t startRead, bool& needRestart) const {
    readUnlockOrRestart(startRead, needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool& needRestart) const {
    needRestart = (startRead != version_lock_obsolete.load());
  }

  // == N.cpp:24-32 ==
  void writeLockOrRestart(bool& needRestart) {
    uint64_t version = readLockOrRestart(needRestart);
    if (needRestart) return;
    upgradeToWriteLockOrRestart(version, needRestart);
  }

  // == N.cpp:34-40 ==
  void upgradeToWriteLockOrRestart(uint64_t& version, bool& needRestart) {
    if (version_lock_obsolete.compare_exchange_strong(version, version + 0b10)) {
      version = version + 0b10;
    } else {
      needRestart = true;
    }
  }

  // == N.cpp:42-44 ==
  void writeUnlock() { version_lock_obsolete.fetch_add(0b10); }
};

// Fanout matches the current (pre-replacement) TLX rep's own default sizing
// for 8-byte pointer keys (tlx::btree_set<const char*, ...>'s traits work
// out to ~32-way leaves / ~16-way inner nodes) -- kept the same magnitude
// so a before/after throughput comparison isn't confounded by a fanout
// change too.
constexpr int kLeafSlots = 32;
constexpr int kInnerSlots = 16;

// Leaf entries are `const char*` pointers into arena-allocated,
// RocksDB-internal-key-encoded buffers (the memtable's value bytes are
// already inlined after the key in that same buffer -- matches the
// current tlx_btree_rep's `btree_set<const char*>` design, so no separate
// value slot is needed here).
struct LeafNode : public NodeHeader {
  const char* keys[kLeafSlots];
  LeafNode* next = nullptr;  // doubly-linked leaf chain, ascending key order
  LeafNode* prev = nullptr;  // (Prev()/reverse iteration needs both links)

  LeafNode() : NodeHeader(/*leaf=*/true) {}
  bool isFull() const { return count >= kLeafSlots; }
};

struct InnerNode : public NodeHeader {
  // separators[0..count-1] partition children[0..count]: children[i] holds
  // keys < separators[i] (for i < count) and children[count] holds keys
  // >= separators[count-1].
  const char* separators[kInnerSlots];
  NodeHeader* children[kInnerSlots + 1];

  InnerNode() : NodeHeader(/*leaf=*/false) {}
  bool isFull() const { return count >= kInnerSlots; }
};

// Permanent sentinel, one per Tree, living inline in the Tree object (never
// arena-allocated, never retired). Reuses NodeHeader's lock so replacing
// the root -- which a real B+Tree must do repeatedly as it grows, unlike
// ART, which pays for a full-size N256 as a permanent root and never
// replaces it (confirmed: third-party/ARTSynchronized/OptimisticLockCoupling/
// Tree.h:18 `N *const root`, Tree.cpp:14) -- is just ordinary lock
// coupling through one more level, not a special atomic-root-pointer
// scheme.
struct RootHolder : public NodeHeader {
  NodeHeader* child = nullptr;
  RootHolder() : NodeHeader(/*leaf=*/false) {}
};

}  // namespace BTree

#endif  // BTREE_NODE_H
