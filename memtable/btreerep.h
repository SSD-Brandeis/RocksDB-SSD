// btreerep.h
//
// Replaces the previous TLXBTreeRep (global rwlock + write-gate around a
// non-thread-safe vendored B+tree) with a fine-grained Optimistic Lock
// Coupling B+Tree (BTree::Tree, memtable/btree/). Factory id 12 / the
// CLI/Python-facing "tlx_btree" name are kept unchanged (see class comment
// in the .cc file); only the internal C++ implementation is new.
#pragma once

#ifndef ROCKSDB_LITE

#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "memtable/btree/Tree.h"

namespace ROCKSDB_NAMESPACE {

// BTreeRepFactory is declared in rocksdb/memtablerep.h alongside the
// other memtable factories; only the BTreeRep implementation lives here.
class BTreeRep : public MemTableRep {
 public:
  explicit BTreeRep(const MemTableRep::KeyComparator& cmp, Allocator* allocator);

  virtual ~BTreeRep() override {}

  virtual KeyHandle Allocate(const size_t len, char** buf) override;

  virtual void Insert(KeyHandle handle) override;

  virtual void InsertWithHint(KeyHandle handle, void** hint) override {
    Insert(handle);
  }

  // BTree::Tree::insert is safe under concurrent callers (lock
  // coupling with per-node write locks on only the contiguous full-ancestor
  // suffix nearest the leaf), so parallel write-group threads may insert
  // directly -- no serialization at the rep level, unlike the previous
  // TLXBTreeRep's global write_gate_ + exclusive rwlock.
  virtual void InsertConcurrently(KeyHandle handle) override { Insert(handle); }

  virtual void InsertWithHintConcurrently(KeyHandle handle, void** hint) override {
    InsertConcurrently(handle);
  }

  virtual bool Contains(const char* key) const override;

  // Not tracked, same pre-existing limitation shared with ARTRep
  // (artrep.cc) and the previous TLXBTreeRep.
  virtual size_t ApproximateMemoryUsage() override { return 0; }

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg, const char* entry)) override;

  virtual MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  virtual MemTableRep::Iterator* GetDynamicPrefixIterator(
      Arena* arena = nullptr) override {
    return GetIterator(arena);
  }

 private:
  friend class BTreeIterator;
  const MemTableRep::KeyComparator& cmp_;
  Allocator* const allocator_;
  BTree::Tree tree_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
