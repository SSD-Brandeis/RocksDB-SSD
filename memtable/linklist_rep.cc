

#include <assert.h>
#include <stddef.h>
#include <atomic>
#include <memory>
#include <vector>
#include <algorithm>

#include "db/memtable.h"
#include "memory/arena.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace {

struct Node {
  Node* next;
  Node* prev;
  char key_data[1];

  const char* Key() const { return key_data; }
  char* MutableKey() { return key_data; }
};

class LinkListRep : public MemTableRep {
 public:
  explicit LinkListRep(const MemTableRep::KeyComparator& compare,
                       Allocator* allocator, const SliceTransform* transform,
                       Logger* logger);

  KeyHandle Allocate(const size_t len, char** buf) override;
  void Insert(KeyHandle handle) override;
  bool Contains(const char* key) const override;
  size_t ApproximateMemoryUsage() override;

 
  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~LinkListRep() override;

  // GetIterator:
  // if immutable, no snapshot creation
  // if mutable, create snapshot and measure snapshot creation time.
  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;
  MemTableRep::Iterator* GetDynamicPrefixIterator(Arena* arena = nullptr) override;

  void MarkReadOnly() override;

  class Iterator : public MemTableRep::Iterator {
   public:
    Iterator(LinkListRep* rep,
             std::shared_ptr<std::vector<const char*>> snapshot,
             const MemTableRep::KeyComparator& compare);

    ~Iterator() override = default;

    bool Valid() const override;
    const char* key() const override;
    void Next() override;
    void Prev() override;

  
    void Seek(const Slice& internal_key, const char* memtable_key) override;
    void SeekForPrev(const Slice& internal_key, const char* memtable_key) override;
    void SeekToFirst() override;
    void SeekToLast() override;

   private:
    LinkListRep* rep_;
    const MemTableRep::KeyComparator& compare_;
    mutable bool sorted_;
    std::string tmp_;
    std::shared_ptr<std::vector<const char*>> snapshot_;
    mutable size_t current_index_;

    void DoSort() const;

    inline const char* EncodeKey(std::string* tmp, const Slice& user_key) const {
      tmp->clear();
      PutLengthPrefixedSlice(tmp, user_key);
      return tmp->data();
    }
  };

 private:
  Node* head_;
  Node* tail_;

  Allocator* const allocator_;
  const MemTableRep::KeyComparator& compare_;
  const SliceTransform* const transform_;
  Logger* const logger_;

  mutable port::RWMutex rwlock_;
  bool immutable_;

  std::vector<Node*> node_pointers_;

  std::shared_ptr<std::vector<const char*>> entries_;
  bool sorted_;

  std::atomic<uint64_t> total_snapshot_creation_time_ns_{0};
  std::atomic<uint64_t> total_sorting_time_ns_{0};

  LinkListRep(const LinkListRep&) = delete;
  LinkListRep& operator=(const LinkListRep&) = delete;
};

LinkListRep::LinkListRep(const MemTableRep::KeyComparator& compare,
                         Allocator* allocator, const SliceTransform* transform,
                         Logger* logger)
    : MemTableRep(allocator),
      head_(nullptr),
      tail_(nullptr),
      allocator_(allocator),
      compare_(compare),
      transform_(transform),
      logger_(logger),
      immutable_(false),
      entries_(nullptr),
      sorted_(false) {}

LinkListRep::~LinkListRep() {

}

KeyHandle LinkListRep::Allocate(const size_t len, char** buf) {
  size_t total_size = sizeof(Node) + len - 1;
  char* mem = allocator_->AllocateAligned(total_size);
  Node* node = reinterpret_cast<Node*>(mem);
  node->next = nullptr;
  node->prev = nullptr;
  *buf = node->MutableKey();
  return static_cast<void*>(node);
}

void LinkListRep::Insert(KeyHandle handle) {

  WriteLock l(&rwlock_);
  assert(!immutable_);

  Node* node = reinterpret_cast<Node*>(handle);
  if (tail_ == nullptr) {
    head_ = node;
    tail_ = node;
  } else {
    tail_->next = node;
    node->prev = tail_;
    tail_ = node;
  }

  node_pointers_.push_back(node);


}

bool LinkListRep::Contains(const char* key) const {
  ReadLock l(&rwlock_);
  Node* current = tail_;
  while (current != nullptr) {
    if (compare_(current->Key(), key) == 0) {
      return true;
    }
    current = current->prev;
  }
  return false;
}

size_t LinkListRep::ApproximateMemoryUsage() {
  return 0;
}

void LinkListRep::Get(const LookupKey& k, void* callback_args,
                      bool (*callback_func)(void* arg, const char* entry)) {


  rwlock_.ReadLock();
  LinkListRep* linklist_rep;
  std::shared_ptr<std::vector<const char*>> snapshot;

  if (immutable_) {
   
    linklist_rep = this;
    snapshot = entries_;
  } else {
    linklist_rep = nullptr;

    snapshot = std::make_shared<std::vector<const char*>>();
    snapshot->resize(node_pointers_.size());
    for (size_t i = 0; i < node_pointers_.size(); i++) {
      (*snapshot)[i] = node_pointers_[i]->Key();
    }

  }

  Iterator iter(linklist_rep, snapshot, compare_);
  rwlock_.ReadUnlock();

  for (iter.Seek(k.user_key(), k.memtable_key().data());
       iter.Valid() && callback_func(callback_args, iter.key());
       iter.Next()) {
  }


}

MemTableRep::Iterator* LinkListRep::GetIterator(Arena* arena) {
  char* mem = nullptr;
  if (arena != nullptr) {
    mem = arena->AllocateAligned(sizeof(Iterator));
  }

  rwlock_.ReadLock();
  LinkListRep* linklist_rep;
  std::shared_ptr<std::vector<const char*>> snapshot;

  if (immutable_) {
    linklist_rep = this;
    snapshot = entries_;
  } else {
    linklist_rep = nullptr;

    snapshot = std::make_shared<std::vector<const char*>>();
    snapshot->resize(node_pointers_.size());
    for (size_t i = 0; i < node_pointers_.size(); i++) {
      (*snapshot)[i] = node_pointers_[i]->Key();
    }

  }

  Iterator* it;
  if (arena == nullptr) {
    it = new Iterator(linklist_rep, snapshot, compare_);
  } else {
    it = new (mem) Iterator(linklist_rep, snapshot, compare_);
  }
  rwlock_.ReadUnlock();
  return it;
}

MemTableRep::Iterator* LinkListRep::GetDynamicPrefixIterator(Arena* arena) {
  return GetIterator(arena);
}

void LinkListRep::MarkReadOnly() {
  WriteLock l(&rwlock_);
  immutable_ = true;


  auto tmp = std::make_shared<std::vector<const char*>>();
  tmp->resize(node_pointers_.size());
  for (size_t i = 0; i < node_pointers_.size(); i++) {
    (*tmp)[i] = node_pointers_[i]->Key();
  }
  entries_ = tmp;
  sorted_ = false;
}

LinkListRep::Iterator::Iterator(LinkListRep* rep,
                                std::shared_ptr<std::vector<const char*>> snapshot,
                                const MemTableRep::KeyComparator& compare)
    : rep_(rep),
      compare_(compare),
      sorted_(false),
      snapshot_(std::move(snapshot)),
      current_index_(0) {}

bool LinkListRep::Iterator::Valid() const {
  if (!sorted_) {
    DoSort();
  }
  return current_index_ < snapshot_->size();
}

const char* LinkListRep::Iterator::key() const {
  assert(Valid());
  return (*snapshot_)[current_index_];
}

void LinkListRep::Iterator::Next() {
  if (Valid()) {
    current_index_++;
  }
}

void LinkListRep::Iterator::Prev() {
  if (!sorted_) {
    DoSort();
  }
  if (current_index_ == 0) {
    current_index_ = snapshot_->size();
  } else {
    current_index_--;
  }
}

void LinkListRep::Iterator::Seek(const Slice& user_key, const char* memtable_key) {
  if (!sorted_) {
    DoSort();
  }
  const char* encoded_key =
      (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
  auto it = std::lower_bound(snapshot_->begin(), snapshot_->end(), encoded_key,
                             [this](const char* a, const char* b) {
                               return compare_(a, b) < 0;
                             });
  current_index_ = static_cast<size_t>(it - snapshot_->begin());
}

void LinkListRep::Iterator::SeekForPrev(const Slice& user_key, const char* memtable_key) {
  if (!sorted_) {
    DoSort();
  }
  const char* encoded_key =
      (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
  auto it = std::upper_bound(snapshot_->begin(), snapshot_->end(), encoded_key,
                             [this](const char* key_ptr, const char* entry) {
                               return compare_(key_ptr, entry) < 0;
                             });
  if (it == snapshot_->begin()) {
    current_index_ = snapshot_->size();
  } else {
    current_index_ = static_cast<size_t>((it - snapshot_->begin()) - 1);
  }
}

void LinkListRep::Iterator::SeekToFirst() {
  if (!sorted_) {
    DoSort();
  }
  current_index_ = 0;
}

void LinkListRep::Iterator::SeekToLast() {
  if (!sorted_) {
    DoSort();
  }
  if (snapshot_->empty()) {
    current_index_ = snapshot_->size();
  } else {
    current_index_ = snapshot_->size() - 1;
  }
}

void LinkListRep::Iterator::DoSort() const {
  if (sorted_) return;


  std::sort(snapshot_->begin(), snapshot_->end(),
            [this](const char* a, const char* b) {
              return compare_(a, b) < 0;
            });


  sorted_ = true;
}

class LinkListRepFactory : public MemTableRepFactory {
 public:
  explicit LinkListRepFactory() {}

  using MemTableRepFactory::CreateMemTableRep;

  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override {
    return new LinkListRep(compare, allocator, transform, logger);
  }

  const char* Name() const override { return "LinkListRepFactory"; }
};

}  // namespace

MemTableRepFactory* NewLinkListRepFactory() {
  return new LinkListRepFactory();
}

}  // namespace ROCKSDB_NAMESPACE
