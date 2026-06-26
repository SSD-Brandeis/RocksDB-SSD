// tlx_btree_rep.cc
#include "memtable/tlx_btree_rep.h"

#ifndef ROCKSDB_LITE

#include "db/dbformat.h"
#include "rocksdb/memtablerep.h"
#include "db/memtable.h"
#include "memory/arena.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

struct KeyComparatorWrapper {
  const MemTableRep::KeyComparator* compare_;

  explicit KeyComparatorWrapper(const MemTableRep::KeyComparator* compare)
      : compare_(compare) {}

  bool operator()(const char* a, const char* b) const {
    return (*compare_)(a, b) < 0;
  }
};

class TLXBTreeRep : public MemTableRep {
 public:
  explicit TLXBTreeRep(const MemTableRep::KeyComparator& cmp, Allocator* allocator)
      : MemTableRep(allocator),
        cmp_(cmp),
        allocator_(allocator),
        tree_(std::make_shared<tlx::btree_set<const char*, KeyComparatorWrapper>>(KeyComparatorWrapper(&cmp_))) {}

  virtual ~TLXBTreeRep() override {}

  virtual KeyHandle Allocate(const size_t len, char** buf) override {
    *buf = allocator_->Allocate(len);
    return static_cast<KeyHandle>(*buf);
  }

  virtual void Insert(KeyHandle handle) override {
    const char* key = static_cast<const char*>(handle);
    tree_->insert(key);
  }

  virtual void InsertWithHint(KeyHandle handle, void** hint) override {
    Insert(handle);
  }

  virtual void InsertConcurrently(KeyHandle handle) override {
    Insert(handle);
  }

  virtual void InsertWithHintConcurrently(KeyHandle handle, void** hint) override {
    InsertConcurrently(handle);
  }

  virtual bool Contains(const char* key) const override {
    return tree_->find(key) != tree_->end();
  }

  virtual size_t ApproximateMemoryUsage() override {
    return 0;
  }

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg, const char* entry)) override {
    const char* target = k.memtable_key().data();
    auto it = tree_->lower_bound(target);
    for (; it != tree_->end() && callback_func(callback_args, *it); ++it) {
    }
  }

  virtual MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  virtual MemTableRep::Iterator* GetDynamicPrefixIterator(Arena* arena = nullptr) override {
    return GetIterator(arena);
  }

 private:
  friend class TLXBTreeIterator;
  const MemTableRep::KeyComparator& cmp_;
  Allocator* const allocator_;
  std::shared_ptr<tlx::btree_set<const char*, KeyComparatorWrapper>> tree_;
};

class TLXBTreeIterator : public MemTableRep::Iterator {
 public:
  explicit TLXBTreeIterator(std::shared_ptr<tlx::btree_set<const char*, KeyComparatorWrapper>> tree,
                            const MemTableRep::KeyComparator& cmp)
      : tree_(tree), cmp_(cmp), iter_(tree_->end()), valid_(false) {}

  virtual ~TLXBTreeIterator() override {}

  virtual bool Valid() const override {
    return valid_;
  }

  virtual const char* key() const override {
    assert(valid_);
    return *iter_;
  }

  virtual void Next() override {
    assert(valid_);
    ++iter_;
    valid_ = (iter_ != tree_->end());
  }

  virtual void Prev() override {
    assert(valid_);
    if (iter_ == tree_->begin()) {
      valid_ = false;
    } else {
      --iter_;
    }
  }

  virtual void Seek(const Slice& user_key, const char* memtable_key) override {
    const char* encoded_key =
        (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
    iter_ = tree_->lower_bound(encoded_key);
    valid_ = (iter_ != tree_->end());
  }

  virtual void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
    const char* encoded_key =
        (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
    iter_ = tree_->upper_bound(encoded_key);
    if (iter_ != tree_->begin()) {
      --iter_;
      valid_ = true;
    } else {
      valid_ = false;
    }
  }

  virtual void SeekToFirst() override {
    iter_ = tree_->begin();
    valid_ = (iter_ != tree_->end());
  }

  virtual void SeekToLast() override {
    iter_ = tree_->end();
    if (iter_ != tree_->begin()) {
      --iter_;
      valid_ = true;
    } else {
      valid_ = false;
    }
  }

 private:
  std::shared_ptr<tlx::btree_set<const char*, KeyComparatorWrapper>> tree_;
  const MemTableRep::KeyComparator& cmp_;
  tlx::btree_set<const char*, KeyComparatorWrapper>::iterator iter_;
  bool valid_;
  std::string tmp_;
};

MemTableRep::Iterator* TLXBTreeRep::GetIterator(Arena* arena) {
  if (arena != nullptr) {
    void* mem = arena->AllocateAligned(sizeof(TLXBTreeIterator));
    return new (mem) TLXBTreeIterator(tree_, cmp_);
  } else {
    return new TLXBTreeIterator(tree_, cmp_);
  }
}

MemTableRep* TLXBTreeRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& cmp, Allocator* allocator,
    const SliceTransform* transform, Logger* logger) {
  return new TLXBTreeRep(cmp, allocator);
}

MemTableRepFactory* NewTLXBTreeRepFactory() {
  return new TLXBTreeRepFactory();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
