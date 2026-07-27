// key_comparator_wrapper.h
//
// Used by olc_btree_rep.cc for internal-key comparison logic.
#pragma once

#ifndef ROCKSDB_LITE

#include "rocksdb/memtablerep.h"

namespace rocksdb {

struct KeyComparatorWrapper {
  const MemTableRep::KeyComparator* compare_;

  explicit KeyComparatorWrapper(const MemTableRep::KeyComparator* compare)
      : compare_(compare) {}

  bool operator()(const char* a, const char* b) const {
    return (*compare_)(a, b) < 0;
  }
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
