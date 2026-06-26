// tlx_btree_rep.h
#pragma once

#ifndef ROCKSDB_LITE

#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "tlx/container/btree_set.hpp"

namespace rocksdb {

class TLXBTreeRepFactory : public MemTableRepFactory {
 public:
  explicit TLXBTreeRepFactory() {}

  virtual ~TLXBTreeRepFactory() {}

  using MemTableRepFactory::CreateMemTableRep;

  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& cmp, Allocator* allocator,
      const SliceTransform* transform, Logger* logger) override;

  virtual const char* Name() const override { return "TLXBTreeRepFactory"; }
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
