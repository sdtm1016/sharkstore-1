#include <gtest/gtest.h>

#include "base/status.h"
#include "base/util.h"
#include "common/ds_encoding.h"
#include "storage/meta_store.h"

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;

class MetaStoreTest : public ::testing::Test {
protected:
    MetaStoreTest() : store_(nullptr) {}

    void SetUp() override {
        char path[] = "/tmp/sharkstore_ds_meta_store_test_XXXXXX";
        char *tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        tmp_dir_ = tmp;

        store_ = new MetaStore(tmp_dir_);
        auto s = store_->Open();
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    void TearDown() override {
        delete store_;
        if (!tmp_dir_.empty()) {
            sharkstore::RemoveDirAll(tmp_dir_.c_str());
        }
    }

protected:
    std::string tmp_dir_;
    MetaStore *store_;
};

// TEST(Meta, Dump) {
//    rocksdb::DB *db = nullptr;
//    rocksdb::Options ops;
////    auto s = rocksdb::DB::OpenForReadOnly(rocksdb::Options(),
///"/home/jonah/meta", &db);
//    auto s = rocksdb::DB::OpenForReadOnly(rocksdb::Options(),
//    "/home/jonah/test/sharkstore/d1/db/meta", &db);
//    ASSERT_TRUE(s.ok()) << s.ToString();
//    auto it = db->NewIterator(rocksdb::ReadOptions());
//    it->SeekToFirst();
//    while (it->Valid()) {
//        std::cout << "key: " <<
//        EncodeToHexString(it->key().ToString().substr(0, 1))
//                  <<  "-" <<it->key().ToString().substr(1) << std::endl;
//        it->Next();
//    }
//}
TEST_F(MetaStoreTest, NodeID) {
    uint64_t node = sharkstore::randomInt();
    auto s = store_->SaveNodeID(node);
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t node2 = 0;
    s = store_->GetNodeID(&node2);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(node2, node);
}

TEST_F(MetaStoreTest, ApplyIndex) {
    uint64_t range_id = sharkstore::randomInt();
    uint64_t applied = 1;
    auto s = store_->LoadApplyIndex(range_id, &applied);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(applied, 0);

    uint64_t save_applied = sharkstore::randomInt();
    s = store_->SaveApplyIndex(range_id, save_applied);
    ASSERT_TRUE(s.ok()) << s.ToString();

    s = store_->LoadApplyIndex(range_id, &applied);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(applied, save_applied);

    s = store_->DeleteApplyIndex(range_id);
    ASSERT_TRUE(s.ok()) << s.ToString();

    s = store_->LoadApplyIndex(range_id, &applied);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(applied, 0);
}
}
