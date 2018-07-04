#include <gtest/gtest.h>

#include "base/util.h"
#include "raft/src/impl/storage/meta_file.h"
#include "test_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::raft::impl;
using namespace sharkstore::raft::impl::storage;
using namespace sharkstore::raft::impl::testutil;
using sharkstore::Status;
using sharkstore::randomInt;

class MetaTest : public ::testing::Test {
protected:
    void SetUp() override {
        char path[] = "/tmp/sharkstore_raft_meta_test_XXXXXX";
        char* tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        tmp_dir_ = tmp;

        meta_file_ = new MetaFile(tmp_dir_);
        auto s = meta_file_->Open();
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    void TearDown() override {
        if (meta_file_ != nullptr) {
            meta_file_->Destroy();
            delete meta_file_;
        }
        if (!tmp_dir_.empty()) {
            std::remove(tmp_dir_.c_str());
        }
    }

protected:
    std::string tmp_dir_;
    MetaFile* meta_file_{nullptr};
};

TEST_F(MetaTest, Empty) {
    pb::HardState hs;
    pb::TruncateMeta tm;
    auto s = meta_file_->Load(&hs, &tm);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(hs.term(), 0);
    ASSERT_EQ(hs.vote(), 0);
    ASSERT_EQ(hs.commit(), 0);
    ASSERT_EQ(tm.index(), 0);
    ASSERT_EQ(tm.term(), 0);
}

TEST_F(MetaTest, SaveLoad) {
    pb::HardState hs;
    hs.set_term(randomInt());
    hs.set_commit(randomInt());
    hs.set_vote(randomInt());

    pb::TruncateMeta tm;
    tm.set_index(randomInt());
    tm.set_term(randomInt());

    // save
    auto s = meta_file_->SaveHardState(hs);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = meta_file_->SaveTruncMeta(tm);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // load
    pb::HardState load_hs;
    pb::TruncateMeta load_tm;
    s = meta_file_->Load(&load_hs, &load_tm);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = Equal(load_hs, hs);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = Equal(load_tm, tm);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(MetaTest, SaveLoad2) {
    pb::HardState hs;

    pb::TruncateMeta tm;
    tm.set_index(randomInt());
    tm.set_term(randomInt());

    // save truncmate only
    auto s = meta_file_->SaveTruncMeta(tm);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // load
    pb::HardState load_hs;
    pb::TruncateMeta load_tm;
    s = meta_file_->Load(&load_hs, &load_tm);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = Equal(load_hs, hs);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = Equal(load_tm, tm);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

}  // namespace