// art_rep.cc
#pragma GCC diagnostic ignored "-Wshadow"
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include "memtable/art_rep.h"

#ifndef ROCKSDB_LITE

#include "db/dbformat.h"
#include "rocksdb/memtablerep.h"
#include <iostream>
#include "db/memtable.h"
#include "memory/arena.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/string_util.h"
#include <endian.h>

namespace rocksdb {

MemTableRep* ARTRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& cmp, Allocator* allocator,
    const SliceTransform* transform, Logger* logger) {
  return new ARTRep(cmp, allocator);
}

MemTableRepFactory* NewARTRepFactory() {
  return new ARTRepFactory();
}

void ARTRep::EncodeARTKey(const char* memtable_key, Key& art_key) {
  uint32_t key_length;
  const char* key_ptr = GetVarint32Ptr(memtable_key, memtable_key + 5, &key_length);
  uint32_t user_key_len = key_length - 8;
  
  art_key.setKeyLen(key_length);
  memcpy(&art_key[0], key_ptr, user_key_len);
  
  uint64_t suffix = DecodeFixed64(key_ptr + user_key_len);
  uint64_t be_suffix = htobe64(~suffix);
  memcpy(&art_key[user_key_len], &be_suffix, 8);
}

void ARTRep::LoadKeyFromTID(TID tid, Key& key) {
  const char* memtable_key = reinterpret_cast<const char*>(tid);
  EncodeARTKey(memtable_key, key);
}

ARTRep::ARTRep(const MemTableRep::KeyComparator& cmp, Allocator* allocator)
    : MemTableRep(allocator), tree_(LoadKeyFromTID), cmp_(cmp), allocator_(allocator) {}

ARTRep::~ARTRep() {}

KeyHandle ARTRep::Allocate(const size_t len, char** buf) {
  *buf = allocator_->Allocate(len);
  return static_cast<KeyHandle>(*buf);
}

void ARTRep::Insert(KeyHandle handle) {
  Key key;
  const char* memtable_key = static_cast<const char*>(handle);
  EncodeARTKey(memtable_key, key);
  auto threadInfo = tree_.getThreadInfo();
  tree_.insert(key, reinterpret_cast<TID>(handle), threadInfo);
}

void ARTRep::InsertConcurrently(KeyHandle handle) {
  Insert(handle);
}

bool ARTRep::Contains(const char* key) const {
  Key art_key;
  EncodeARTKey(key, art_key);
  auto threadInfo = const_cast<ARTRep*>(this)->tree_.getThreadInfo();
  TID res = tree_.lookup(art_key, threadInfo);
  return res != 0;
}

size_t ARTRep::ApproximateMemoryUsage() {
  return 0; 
}

void ARTRep::Get(const LookupKey& k, void* callback_args,
                 bool (*callback_func)(void* arg, const char* entry)) {
  Key start_key;
  EncodeARTKey(k.memtable_key().data(), start_key);
  
  TID results[64];
  std::size_t resultsFound = 0;
  auto threadInfo = const_cast<ARTRep*>(this)->tree_.getThreadInfo();
  
  const_cast<ARTRep*>(this)->tree_.lookupRange(start_key, results, 64, resultsFound, threadInfo);
  
  for (size_t i = 0; i < resultsFound; ++i) {
    const char* entry = reinterpret_cast<const char*>(results[i]);
    if (!callback_func(callback_args, entry)) {
      break;
    }
  }
}

class ARTIterator : public MemTableRep::Iterator {
 public:
  explicit ARTIterator(const ART_OLC::Tree* tree, const MemTableRep::KeyComparator& cmp)
      : tree_(tree), cmp_(cmp), valid_(false), buffer_idx_(0), buffer_len_(0), reverse_(false) {
    threadInfo_ = new ART::ThreadInfo(const_cast<ART_OLC::Tree*>(tree_)->getThreadInfo());
  }
  
  virtual ~ARTIterator() override {
    delete threadInfo_;
  }

  virtual bool Valid() const override { return valid_; }

  virtual const char* key() const override {
    assert(valid_);
    return reinterpret_cast<const char*>(buffer_[buffer_idx_]);
  }

  virtual void Next() override {
    assert(valid_);
    buffer_idx_++;
    if (buffer_idx_ < buffer_len_) {
      return;
    }
    Key current_key;
    const char* last_entry = reinterpret_cast<const char*>(buffer_[buffer_len_ - 1]);
    ARTRep::EncodeARTKey(last_entry, current_key);
    FetchNext(current_key);
  }

  virtual void Prev() override {
    assert(valid_);
    buffer_idx_++; 
    if (reverse_ && buffer_idx_ < buffer_len_) {
      return;
    }
    Key current_key;
    const char* last_entry;
    if (!reverse_) {
      last_entry = reinterpret_cast<const char*>(buffer_[0]);
    } else {
      last_entry = reinterpret_cast<const char*>(buffer_[buffer_len_ - 1]);
    }
    ARTRep::EncodeARTKey(last_entry, current_key);
    FetchPrev(current_key);
  }

  virtual void Seek(const Slice& internal_key, const char* memtable_key) override {
    Key start_key;
    if (memtable_key != nullptr) {
      ARTRep::EncodeARTKey(memtable_key, start_key);
    } else {
      start_key.setKeyLen(internal_key.size());
      uint32_t user_key_len = internal_key.size() - 8;
      memcpy(&start_key[0], internal_key.data(), user_key_len);
      uint64_t suffix = DecodeFixed64(internal_key.data() + user_key_len);
      uint64_t be_suffix = htobe64(~suffix);
      memcpy(&start_key[user_key_len], &be_suffix, 8);
    }
    FetchNext(start_key, true);
  }

  virtual void SeekForPrev(const Slice& internal_key, const char* memtable_key) override {
    Key start_key;
    if (memtable_key != nullptr) {
      ARTRep::EncodeARTKey(memtable_key, start_key);
    } else {
      start_key.setKeyLen(internal_key.size());
      uint32_t user_key_len = internal_key.size() - 8;
      memcpy(&start_key[0], internal_key.data(), user_key_len);
      uint64_t suffix = DecodeFixed64(internal_key.data() + user_key_len);
      uint64_t be_suffix = htobe64(~suffix);
      memcpy(&start_key[user_key_len], &be_suffix, 8);
    }
    FetchPrev(start_key, true);
  }

  virtual void SeekToFirst() override {
    Key empty_key;
    empty_key.setKeyLen(0);
    FetchNext(empty_key, true);
  }

  virtual void SeekToLast() override {
    Key max_key;
    char max_bytes[256];
    memset(max_bytes, 0xff, 256);
    max_key.setKeyLen(256);
    memcpy(&max_key[0], max_bytes, 256);
    FetchPrev(max_key, true);
  }

 private:
  void FetchNext(const Key& start_key, bool inclusive = false) {
    reverse_ = false;
    buffer_len_ = 0;
    buffer_idx_ = 0;
    valid_ = false;

    TID temp_buffer[64];
    std::size_t found = 0;
    tree_->lookupRange(start_key, temp_buffer, 64, found, *threadInfo_);

    for (size_t i = 0; i < found; ++i) {
      const char* entry = reinterpret_cast<const char*>(temp_buffer[i]);
      Key entry_key;
      ARTRep::EncodeARTKey(entry, entry_key);
      
      if (!inclusive && entry_key == start_key) {
        continue;
      }
      buffer_[buffer_len_++] = temp_buffer[i];
    }
    if (buffer_len_ > 0) {
      valid_ = true;
    }
  }

  void FetchPrev(const Key& start_key, bool inclusive = false) {
    reverse_ = true;
    buffer_len_ = 0;
    buffer_idx_ = 0;
    valid_ = false;

    TID temp_buffer[64];
    std::size_t found = 0;
    tree_->lookupRangeReverse(start_key, temp_buffer, 64, found, *threadInfo_);

    for (size_t i = 0; i < found; ++i) {
      const char* entry = reinterpret_cast<const char*>(temp_buffer[i]);
      Key entry_key;
      ARTRep::EncodeARTKey(entry, entry_key);
      
      if (!inclusive && entry_key == start_key) {
        continue;
      }
      buffer_[buffer_len_++] = temp_buffer[i];
    }
    if (buffer_len_ > 0) {
      valid_ = true;
    }
  }

  const ART_OLC::Tree* tree_;
  const MemTableRep::KeyComparator& cmp_;
  bool valid_;
  size_t buffer_idx_;
  size_t buffer_len_;
  bool reverse_;
  TID buffer_[64];
  ART::ThreadInfo* threadInfo_;
};

MemTableRep::Iterator* ARTRep::GetIterator(Arena* arena) {
  if (arena != nullptr) {
    void* mem = arena->AllocateAligned(sizeof(ARTIterator));
    return new (mem) ARTIterator(&tree_, cmp_);
  } else {
    return new ARTIterator(&tree_, cmp_);
  }
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
