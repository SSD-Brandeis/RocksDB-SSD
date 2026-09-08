// Dynamic memtable factory — SSD-Lab custom implementation.
//
// Consults a MemtableAdvisor (injected from outside the library) on every
// CreateMemTableRep() call and delegates to whichever of the nine built-in
// factory implementations the advisor recommends for the current workload.

#include <atomic>
#include <memory>

#include "logging/logging.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"

namespace ROCKSDB_NAMESPACE {

MemTableRepFactory* NewOLCBTreeRepFactory();

namespace {

// Returns true for the type ids whose factories declare concurrent-insert
// support (i.e. IsInsertConcurrentlySupported() == true).
static bool TypeSupportsConcurrentInsert(int type) {
  switch (type) {
    case 1: // SkipList
    case 2: // VectorRep
    case 5: // UnsortedVector
    case 6: // SortedVector
    case 7: // LinkList
    case 8: // SimpleSkipList
    case 11: // ART
    case 12: // BTree
      return true;
    default:
      return false;
  }
}

static const char* TypeName(int type) {
  switch (type) {
    case 1: return "SkipList";
    case 2: return "VectorRep";
    case 3: return "HashSkipList";
    case 4: return "HashLinkList";
    case 5: return "UnsortedVector";
    case 6: return "SortedVector";
    case 7: return "LinkList";
    case 8: return "SimpleSkipList";
    case 9: return "HashVector";
    case 11: return "ART";
    case 12: return "BTree";
    default: return "Unknown";
  }
}

class DynamicMemtableFactory : public MemTableRepFactory {
 public:
  DynamicMemtableFactory(MemtableAdvisor* advisor,
                         const DynamicMemtableConfig& cfg)
      : advisor_(advisor), cfg_(cfg), last_type_(1 /* SkipList */) {}

  static const char* kClassName() { return "DynamicMemtableFactory"; }
  const char* Name() const override { return kClassName(); }

  using MemTableRepFactory::CreateMemTableRep;

  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& cmp,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override {
    const bool has_prefix = (transform != nullptr);
    const int type = advisor_->SelectMemtableType(has_prefix);
    last_type_.store(type, std::memory_order_relaxed);

    ROCKS_LOG_INFO(logger, "[DynamicMemtableFactory] selected %s%s",
                   TypeName(type),
                   has_prefix ? " (prefix extractor active)" : "");

    auto sub = MakeSubFactory(type, has_prefix);
    return sub->CreateMemTableRep(cmp, allocator, transform, logger);
  }

  // Conservative: concurrent-insert support is derived from the last type
  // the advisor selected.  On the very first call (before any memtable is
  // created) we assume SkipList (type 1), which does support it.
  bool IsInsertConcurrentlySupported() const override {
    return TypeSupportsConcurrentInsert(
        last_type_.load(std::memory_order_relaxed));
  }

 private:
  MemtableAdvisor*          advisor_;
  const DynamicMemtableConfig cfg_;
  mutable std::atomic<int>  last_type_;

  // Builds a one-shot sub-factory for the given type id.
  // Falls back to SkipList when a prefix-required type is requested but no
  // prefix extractor is active.
  std::unique_ptr<MemTableRepFactory> MakeSubFactory(int type,
                                                     bool has_prefix) const {
    switch (type) {
      case 2:
        return std::make_unique<VectorRepFactory>(cfg_.vector_prealloc);

      case 3:
        if (!has_prefix) break;
        return std::unique_ptr<MemTableRepFactory>(
            NewHashSkipListRepFactory(cfg_.bucket_count, cfg_.skiplist_height,
                                     cfg_.skiplist_branch));

      case 4:
        if (!has_prefix) break;
        return std::unique_ptr<MemTableRepFactory>(
            NewHashLinkListRepFactory(cfg_.bucket_count,
                                     cfg_.huge_page_tlb_size,
                                     cfg_.linklist_log_threshold,
                                     cfg_.linklist_log_dist,
                                     cfg_.linklist_use_skiplist));

      case 5:
        return std::make_unique<UnsortedVectorRepFactory>(cfg_.vector_prealloc);

      case 6:
        return std::make_unique<SortedVectorRepFactory>(cfg_.vector_prealloc);

      case 7:
        return std::make_unique<LinkListRepFactory>();

      case 8:
        return std::make_unique<SimpleSkipListFactory>();

      case 9:
        if (!has_prefix) break;
        return std::unique_ptr<MemTableRepFactory>(
            NewHashVectorRepFactory(cfg_.bucket_count));

      case 11:
        return std::make_unique<ARTRepFactory>();

      case 12:
        return std::unique_ptr<MemTableRepFactory>(NewOLCBTreeRepFactory());

      default:
        break;
    }
    // Case 1 (SkipList) and all fallbacks land here.
    return std::make_unique<SkipListFactory>();
  }
};

}  // namespace

MemTableRepFactory* NewDynamicMemTableFactory(MemtableAdvisor* advisor,
                                              const DynamicMemtableConfig& cfg) {
  return new DynamicMemtableFactory(advisor, cfg);
}

}  // namespace ROCKSDB_NAMESPACE
