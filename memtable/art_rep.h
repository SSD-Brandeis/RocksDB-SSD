// art_rep.h
#pragma once

#ifndef ROCKSDB_LITE

#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "ARTSynchronized/OptimisticLockCoupling/Tree.h"
#include "ARTSynchronized/Key.h"

namespace rocksdb {

class ARTRepFactory : public MemTableRepFactory {
 public:
  explicit ARTRepFactory() {}

  virtual ~ARTRepFactory() {}

  using MemTableRepFactory::CreateMemTableRep;

  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& cmp, Allocator* allocator,
      const SliceTransform* transform, Logger* logger) override;

  virtual const char* Name() const override { return "ARTRepFactory"; }
};

class ARTRep : public MemTableRep {
 public:
  explicit ARTRep(const MemTableRep::KeyComparator& cmp, Allocator* allocator);

  virtual ~ARTRep() override;

  virtual KeyHandle Allocate(const size_t len, char** buf) override;

  virtual void Insert(KeyHandle handle) override;

  virtual void InsertWithHint(KeyHandle handle, void** hint) override { Insert(handle); }

  virtual void InsertConcurrently(KeyHandle handle) override;

  virtual void InsertWithHintConcurrently(KeyHandle handle, void** hint) override { InsertConcurrently(handle); }

  virtual bool Contains(const char* key) const override;

  virtual size_t ApproximateMemoryUsage() override;

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg, const char* entry)) override;

  virtual MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  virtual MemTableRep::Iterator* GetDynamicPrefixIterator(
      Arena* arena = nullptr) override {
    return GetIterator(arena);
  }

  // ART specific encoding
  static void EncodeARTKey(const char* memtable_key, Key& art_key);
  static void LoadKeyFromTID(TID tid, Key& key);

 private:
  ART_OLC::Tree tree_;
  const MemTableRep::KeyComparator& cmp_;
  Allocator* const allocator_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
