#include <gtest/gtest.h>

#include "base/status.h"
#include "base/util.h"
#include "storage/store.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;

class StoreTest : public ::testing::Test {
protected:
    StoreTest() : db_(nullptr), store_(nullptr) {}

    void SetUp() override {
        char path[] = "/tmp/sharkstore_ds_store_test_XXXXXX";
        char* tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        tmp_dir_ = tmp;

        rocksdb::Options ops;
        ops.create_if_missing = true;
        ops.error_if_exists = true;
        auto s = rocksdb::DB::Open(ops, tmp, &db_);
        ASSERT_TRUE(s.ok());

        metapb::Range meta;
        meta.set_id(1);
        meta.set_start_key(std::string("\x00", 1));
        meta.set_end_key("\xff");
        store_ = new Store(meta, db_);
    }

    void TearDown() override {
        delete store_;
        delete db_;
        if (!tmp_dir_.empty()) {
            DestroyDB(tmp_dir_, rocksdb::Options());
        }
    }

protected:
    std::string tmp_dir_;
    rocksdb::DB* db_;
    Store* store_;
};

TEST_F(StoreTest, KeyValue) {
    // test put and get
    std::string key = sharkstore::randomString(32);
    std::string value = sharkstore::randomString(64);
    auto s = store_->Put(key, value);
    ASSERT_TRUE(s.ok());

    std::string actual_value;
    s = store_->Get(key, &actual_value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(actual_value, value);

    // test delete and get
    s = store_->Delete(key);
    ASSERT_TRUE(s.ok());
    s = store_->Get(key, &actual_value);
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.code(), sharkstore::Status::kNotFound);
}

} /* namespace  */
