// btreerep.cc
//
// See btreerep.h and memtable/btree/Tree.cpp for the full design
// rationale (replacing TLXBTreeRep's global rwlock + write-gate with a
// fine-grained Optimistic Lock Coupling B+Tree).

#include "memtable/btreerep.h"

#ifndef ROCKSDB_LITE

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/stl_wrappers.h"
#include "rocksdb/memtablerep.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Glue between BTree's dependency-free runtime-callback interface
// (LessFunc/AllocFunc, see memtable/btree/Tree.h) and RocksDB's actual
// comparator/allocator types, so memtable/btree stays independent of
// RocksDB (mirroring third-party/ARTSynchronized's own independence).
bool LessAdapter(void* ctx, const char* a, const char* b) {
  return (*static_cast<const stl_wrappers::Compare*>(ctx))(a, b);
}

void* NodeAllocAdapter(void* ctx, size_t size) {
  return static_cast<Allocator*>(ctx)->AllocateAligned(size);
}

}  // namespace

BTreeRep::BTreeRep(const MemTableRep::KeyComparator& cmp, Allocator* allocator)
    : MemTableRep(allocator),
      cmp_(cmp),
      allocator_(allocator),
      tree_(&LessAdapter, new stl_wrappers::Compare(cmp_), &NodeAllocAdapter, allocator) {}

// The stl_wrappers::Compare passed to Tree's constructor above is
// intentionally heap-allocated with `new` and never freed: Tree only
// stores the raw `void*` context pointer and never owns it, and it must
// outlive the Tree (i.e. the whole memtable's lifetime) -- allocating it
// from the rep's own Allocator like every other piece of this memtable's
// state would work too, but a single ~24-byte fixed allocation for the
// rep's lifetime is not worth adding an extra AllocFunc call for.

KeyHandle BTreeRep::Allocate(const size_t len, char** buf) {
  *buf = allocator_->Allocate(len);
  return static_cast<KeyHandle>(*buf);
}

void BTreeRep::Insert(KeyHandle handle) {
  const char* key = static_cast<const char*>(handle);
  tree_.insert(key);
}

bool BTreeRep::Contains(const char* key) const { return tree_.contains(key); }

void BTreeRep::Get(const LookupKey& k, void* callback_args,
                     bool (*callback_func)(void* arg, const char* entry)) {
  const char* target = k.memtable_key().data();
  tree_.lookupRange(target, callback_args, callback_func);
}

class BTreeIterator : public MemTableRep::Iterator {
 public:
  explicit BTreeIterator(BTreeRep* rep) : rep_(rep) {}

  virtual ~BTreeIterator() override {}

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
  BTreeRep* rep_;
  BTree::Tree::Cursor cursor_;
  std::string tmp_;
};

MemTableRep::Iterator* BTreeRep::GetIterator(Arena* arena) {
  if (arena != nullptr) {
    void* mem = arena->AllocateAligned(sizeof(BTreeIterator));
    return new (mem) BTreeIterator(this);
  } else {
    return new BTreeIterator(this);
  }
}

MemTableRep* BTreeRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& cmp, Allocator* allocator,
    const SliceTransform* transform, Logger* logger) {
  return new BTreeRep(cmp, allocator);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
