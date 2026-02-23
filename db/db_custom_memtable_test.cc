//  Custom implementation tests from SSD-Lab
//
#include <memory>
#include <string>

#include "db/db_test_util.h"
#include "db/memtable.h"

namespace ROCKSDB_NAMESPACE {
class DBMemTableTest : public DBTestBase {
 public:
  DBMemTableTest() : DBTestBase("db_memtable_test", /*env_do_fsync=*/true) {}
};

TEST_F(DBMemTableTest, SortedVectorConcurrentInsert) {
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.allow_concurrent_memtable_write = true;
  options.memtable_factory.reset(new SortedVectorRepFactory());
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf1"}, options);

  // Multi-threaded writes
  {
    WriteOptions write_options;
    std::vector<port::Thread> threads;
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&, i]() {
        int start = i * 100;
        int end = start + 100;
        WriteBatch batch;
        for (int j = start; j < end; ++j) {
          ASSERT_OK(
              batch.Put(handles_[0], Key(j), "value" + std::to_string(j)));
        }
        ASSERT_OK(db_->Write(write_options, &batch));
      });
    }
    for (auto& t : threads) {
      t.join();
    }

    std::unique_ptr<Iterator> iter(
        db_->NewIterator(ReadOptions(), handles_[0]));
    iter->SeekToFirst();
    for (int i = 0; i < 1000; ++i) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), Key(i));
      ASSERT_EQ(iter->value().ToString(), "value" + std::to_string(i));
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Multi-threaded writes, multi CF
  {
    WriteOptions write_options;
    std::vector<port::Thread> threads;
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&, i]() {
        int start = i * 100;
        int end = start + 100;
        WriteBatch batch;
        for (int j = start; j < end; ++j) {
          ASSERT_OK(batch.Put(handles_[0], Key(j), "CF0" + std::to_string(j)));
          ASSERT_OK(batch.Put(handles_[1], Key(j), "CF1" + std::to_string(j)));
        }
        ASSERT_OK(db_->Write(write_options, &batch));
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    std::unique_ptr<Iterator> iter0(
        db_->NewIterator(ReadOptions(), handles_[0]));
    std::unique_ptr<Iterator> iter1(
        db_->NewIterator(ReadOptions(), handles_[1]));
    iter0->SeekToFirst();
    iter1->SeekToFirst();
    for (int i = 0; i < 1000; ++i) {
      ASSERT_TRUE(iter0->Valid());
      ASSERT_EQ(iter0->key().ToString(), Key(i));
      ASSERT_EQ(iter0->value().ToString(), "CF0" + std::to_string(i));
      iter0->Next();

      ASSERT_TRUE(iter1->Valid());
      ASSERT_EQ(iter1->key().ToString(), Key(i));
      ASSERT_EQ(iter1->value().ToString(), "CF1" + std::to_string(i));
      iter1->Next();
    }
    ASSERT_FALSE(iter0->Valid());
    ASSERT_OK(iter0->status());
    ASSERT_FALSE(iter1->Valid());
    ASSERT_OK(iter1->status());
  }

  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));
}

TEST_F(DBMemTableTest, UnsortedVectorConcurrentInsert) {
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.allow_concurrent_memtable_write = true;
  options.memtable_factory.reset(new UnsortedVectorRepFactory());
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf1"}, options);

  // Multi-threaded writes
  {
    WriteOptions write_options;
    std::vector<port::Thread> threads;
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&, i]() {
        int start = i * 100;
        int end = start + 100;
        WriteBatch batch;
        for (int j = start; j < end; ++j) {
          ASSERT_OK(
              batch.Put(handles_[0], Key(j), "value" + std::to_string(j)));
        }
        ASSERT_OK(db_->Write(write_options, &batch));
      });
    }
    for (auto& t : threads) {
      t.join();
    }

    std::unique_ptr<Iterator> iter(
        db_->NewIterator(ReadOptions(), handles_[0]));
    iter->SeekToFirst();
    for (int i = 0; i < 1000; ++i) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), Key(i));
      ASSERT_EQ(iter->value().ToString(), "value" + std::to_string(i));
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Multi-threaded writes, multi CF
  {
    WriteOptions write_options;
    std::vector<port::Thread> threads;
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&, i]() {
        int start = i * 100;
        int end = start + 100;
        WriteBatch batch;
        for (int j = start; j < end; ++j) {
          ASSERT_OK(batch.Put(handles_[0], Key(j), "CF0" + std::to_string(j)));
          ASSERT_OK(batch.Put(handles_[1], Key(j), "CF1" + std::to_string(j)));
        }
        ASSERT_OK(db_->Write(write_options, &batch));
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    std::unique_ptr<Iterator> iter0(
        db_->NewIterator(ReadOptions(), handles_[0]));
    std::unique_ptr<Iterator> iter1(
        db_->NewIterator(ReadOptions(), handles_[1]));
    iter0->SeekToFirst();
    iter1->SeekToFirst();
    for (int i = 0; i < 1000; ++i) {
      ASSERT_TRUE(iter0->Valid());
      ASSERT_EQ(iter0->key().ToString(), Key(i));
      ASSERT_EQ(iter0->value().ToString(), "CF0" + std::to_string(i));
      iter0->Next();

      ASSERT_TRUE(iter1->Valid());
      ASSERT_EQ(iter1->key().ToString(), Key(i));
      ASSERT_EQ(iter1->value().ToString(), "CF1" + std::to_string(i));
      iter1->Next();
    }
    ASSERT_FALSE(iter0->Valid());
    ASSERT_OK(iter0->status());
    ASSERT_FALSE(iter1->Valid());
    ASSERT_OK(iter1->status());
  }

  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
