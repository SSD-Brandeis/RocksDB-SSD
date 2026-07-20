
//  Writer-preferring reader-writer mutex for memtable reps.
//
//  glibc's default pthread_rwlock (used by port::RWMutex) prefers readers: as
//  long as any reader holds the lock, new readers are admitted ahead of a
//  waiting writer. Memtable reps that serve continuous concurrent reads
//  (point lookups and iterator snapshots that hold the read lock while
//  copying the bucket) can starve the write path almost completely under
//  that policy. This wrapper selects the writer-preferring kind so inserts
//  make progress under read-heavy concurrency. Read locks must not be taken
//  recursively (no rep does).
#pragma once

#include <pthread.h>

namespace ROCKSDB_NAMESPACE {

class WPRWMutex {
 public:
  WPRWMutex() {
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
#if defined(PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)
    pthread_rwlockattr_setkind_np(
        &attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif
    pthread_rwlock_init(&lock_, &attr);
    pthread_rwlockattr_destroy(&attr);
  }

  WPRWMutex(const WPRWMutex&) = delete;
  WPRWMutex& operator=(const WPRWMutex&) = delete;

  ~WPRWMutex() { pthread_rwlock_destroy(&lock_); }

  void ReadLock() { pthread_rwlock_rdlock(&lock_); }
  void WriteLock() { pthread_rwlock_wrlock(&lock_); }
  void ReadUnlock() { pthread_rwlock_unlock(&lock_); }
  void WriteUnlock() { pthread_rwlock_unlock(&lock_); }

 private:
  pthread_rwlock_t lock_;
};

class WPReadLock {
 public:
  explicit WPReadLock(WPRWMutex* mu) : mu_(mu) { mu_->ReadLock(); }
  WPReadLock(const WPReadLock&) = delete;
  WPReadLock& operator=(const WPReadLock&) = delete;
  ~WPReadLock() { mu_->ReadUnlock(); }

 private:
  WPRWMutex* const mu_;
};

class WPWriteLock {
 public:
  explicit WPWriteLock(WPRWMutex* mu) : mu_(mu) { mu_->WriteLock(); }
  WPWriteLock(const WPWriteLock&) = delete;
  WPWriteLock& operator=(const WPWriteLock&) = delete;
  ~WPWriteLock() { mu_->WriteUnlock(); }

 private:
  WPRWMutex* const mu_;
};

}  // namespace ROCKSDB_NAMESPACE
