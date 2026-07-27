// olc_btree_rep.cc
//
// See olc_btree_rep.h and lib/BTreeOLC/Tree.cpp for the full design
// rationale (replacing TLXBTreeRep's global rwlock + write-gate with a
// fine-grained Optimistic Lock Coupling B+Tree).

#include "memtable/olc_btree_rep.h"

#ifndef ROCKSDB_LITE

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/key_comparator_wrapper.h"
#include "rocksdb/memtablerep.h"

namespace rocksdb {

namespace {

// Glue between BTreeOLC's dependency-free runtime-callback interface
// (LessFunc/AllocFunc, see lib/BTreeOLC/Tree.h) and RocksDB's actual
// comparator/allocator types, so lib/BTreeOLC stays independent of
// RocksDB (mirroring lib/ARTSynchronized's own independence).
bool LessAdapter(void* ctx, const char* a, const char* b) {
  return (*static_cast<const KeyComparatorWrapper*>(ctx))(a, b);
}

void* NodeAllocAdapter(void* ctx, size_t size) {
  return static_cast<Allocator*>(ctx)->AllocateAligned(size);
}

}  // namespace

OLCBTreeRep::OLCBTreeRep(const MemTableRep::KeyComparator& cmp, Allocator* allocator)
    : MemTableRep(allocator),
      cmp_(cmp),
      allocator_(allocator),
      tree_(&LessAdapter, new KeyComparatorWrapper(&cmp_), &NodeAllocAdapter, allocator) {}

// The KeyComparatorWrapper passed to Tree's constructor above is
// intentionally heap-allocated with `new` and never freed: Tree only
// stores the raw `void*` context pointer and never owns it, and it must
// outlive the Tree (i.e. the whole memtable's lifetime) -- allocating it
// from the rep's own Allocator like every other piece of this memtable's
// state would work too, but a single ~24-byte fixed allocation for the
// rep's lifetime is not worth adding an extra AllocFunc call for.

KeyHandle OLCBTreeRep::Allocate(const size_t len, char** buf) {
  *buf = allocator_->Allocate(len);
  return static_cast<KeyHandle>(*buf);
}

void OLCBTreeRep::Insert(KeyHandle handle) {
  const char* key = static_cast<const char*>(handle);
  tree_.insert(key);
}

bool OLCBTreeRep::Contains(const char* key) const { return tree_.contains(key); }

void OLCBTreeRep::Get(const LookupKey& k, void* callback_args,
                     bool (*callback_func)(void* arg, const char* entry)) {
  const char* target = k.memtable_key().data();
  tree_.lookupRange(target, callback_args, callback_func);
}

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
  BTreeOLC::Tree::Cursor cursor_;
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

MemTableRep* OLCBTreeRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& cmp, Allocator* allocator,
    const SliceTransform* transform, Logger* logger) {
  return new OLCBTreeRep(cmp, allocator);
}

MemTableRepFactory* NewOLCBTreeRepFactory() {
  return new OLCBTreeRepFactory();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
