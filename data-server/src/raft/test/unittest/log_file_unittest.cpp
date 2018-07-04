#include <gtest/gtest.h>

#include "base/util.h"
#include "raft/src/impl/storage/log_file.h"
#include "test_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::raft::impl;
using namespace sharkstore::raft::impl::testutil;
using namespace sharkstore::raft::impl::storage;
using sharkstore::Status;
using sharkstore::randomInt;

class LogFileTest : public ::testing::Test {
protected:
    void SetUp() override {
        char path[] = "/tmp/sharkstore_raft_log_test_XXXXXX";
        char* tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        tmp_dir_ = tmp;

        log_file_ = new LogFile(tmp_dir_, 1, 1);
        auto s = log_file_->Open(false);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    void TearDown() override {
        if (log_file_ != nullptr) {
            auto s = log_file_->Destroy();
            ASSERT_TRUE(s.ok()) << s.ToString();
            delete log_file_;
        }
        if (!tmp_dir_.empty()) {
            std::remove(tmp_dir_.c_str());
        }
    }

    void ReOpen(bool last_one) {
        auto s = log_file_->Close();
        ASSERT_TRUE(s.ok()) << s.ToString();
        delete log_file_;
        log_file_ = new LogFile(tmp_dir_, 1, 1);
        s = log_file_->Open(false, last_one);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

protected:
    std::string tmp_dir_;
    LogFile* log_file_{nullptr};
};

TEST(LogFormat, FileName) {
    auto filename = makeLogFileName(9, 18);
    ASSERT_EQ(filename, "0000000000000009-0000000000000012.log");
    uint64_t seq = 0, index = 0;
    ASSERT_TRUE(parseLogFileName(filename, seq, index));
    ASSERT_EQ(seq, 9);
    ASSERT_EQ(index, 18);

    filename = makeLogFileName(std::numeric_limits<uint64_t>::max(),
                               std::numeric_limits<uint64_t>::max());
    ASSERT_EQ(filename, "ffffffffffffffff-ffffffffffffffff.log");
    ASSERT_TRUE(parseLogFileName(filename, seq, index));
    ASSERT_EQ(seq, std::numeric_limits<uint64_t>::max());
    ASSERT_EQ(index, std::numeric_limits<uint64_t>::max());

    for (int i = 0; i < 10; ++i) {
        uint64_t rseq = randomInt();
        uint64_t rindex = randomInt();
        filename = makeLogFileName(rseq, rindex);
        ASSERT_TRUE(parseLogFileName(filename, seq, index));
        ASSERT_EQ(seq, rseq);
        ASSERT_EQ(index, rindex);
    }
}

TEST_F(LogFileTest, AppendAndGet) {
    std::vector<EntryPtr> entries;
    for (uint64_t i = 1; i <= 10; ++i) {
        auto e = RandomEntry(i);
        entries.push_back(e);
        auto s = log_file_->Append(e);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    auto s = log_file_->Flush();
    ASSERT_TRUE(s.ok()) << s.ToString();

    ASSERT_EQ(log_file_->Seq(), 1);
    ASSERT_EQ(log_file_->LogSize(), 10);
    ASSERT_EQ(log_file_->Index(), 1);
    ASSERT_EQ(log_file_->LastIndex(), 10);

    for (uint64_t i = 1; i <= 10; ++i) {
        EntryPtr e;
        auto s = log_file_->Get(i, &e);
        ASSERT_TRUE(s.ok()) << s.ToString();
        s = Equal(e, entries[i - 1]);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    for (uint64_t i = 1; i <= 10; ++i) {
        uint64_t term = 0;
        auto s = log_file_->Term(i, &term);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(term, entries[i - 1]->term());
    }
}

TEST_F(LogFileTest, AppendConflict) {
    std::vector<EntryPtr> entries;
    for (uint64_t i = 1; i <= 10; ++i) {
        auto e = RandomEntry(i);
        entries.push_back(e);
        auto s = log_file_->Append(e);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    auto s = log_file_->Flush();
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto e = RandomEntry(5);
    s = log_file_->Append(e);
    ASSERT_TRUE(s.ok()) << s.ToString();
    entries[4] = e;
    s = log_file_->Flush();
    ASSERT_TRUE(s.ok()) << s.ToString();

    ASSERT_EQ(log_file_->LogSize(), 5);
    ASSERT_EQ(log_file_->LastIndex(), 5);

    for (uint64_t i = 1; i <= 5; ++i) {
        EntryPtr e;
        auto s = log_file_->Get(i, &e);
        ASSERT_TRUE(s.ok()) << s.ToString();
        s = Equal(e, entries[i - 1]);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

TEST_F(LogFileTest, Recover) {
    std::vector<EntryPtr> entries;
    for (uint64_t i = 1; i <= 10; ++i) {
        auto e = RandomEntry(i);
        entries.push_back(e);
        auto s = log_file_->Append(e);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    ReOpen(true);
    for (uint64_t i = 1; i <= 10; ++i) {
        EntryPtr e;
        auto s = log_file_->Get(i, &e);
        ASSERT_TRUE(s.ok()) << s.ToString();
        s = Equal(e, entries[i - 1]);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // rotate and recover
    auto s = log_file_->Rotate();
    ASSERT_TRUE(s.ok()) << s.ToString();
    ReOpen(false);
    for (uint64_t i = 1; i <= 10; ++i) {
        EntryPtr e;
        auto s = log_file_->Get(i, &e);
        ASSERT_TRUE(s.ok()) << s.ToString();
        s = Equal(e, entries[i - 1]);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

}  // namespace
