#include <gtest/gtest.h>

#include "base/util.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "raft/src/impl/storage/storage_disk.h"
#include "test_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::raft::impl;
using namespace sharkstore::raft::impl::storage;
using namespace sharkstore::raft::impl::testutil;

class StorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        char path[] = "/tmp/sharkstore_raft_storage_test_XXXXXX";
        char* tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        tmp_dir_ = tmp;

        Open();
    }

    void ReOpen() {
        auto s = storage_->Close();
        ASSERT_TRUE(s.ok()) << s.ToString();
        delete storage_;

        Open();
    }

    void TearDown() override {
        if (storage_ != nullptr) {
            auto s = storage_->Destroy();
            ASSERT_TRUE(s.ok()) << s.ToString();
            delete storage_;
        }
    }

    void SetKeepSize(size_t size) {
        keep_size_ = size;
        ReOpen();
    }

private:
    void Open() {
        DiskStorage::Options ops;
        ops.log_file_size = 1024;
        ops.max_log_files = keep_size_;
        ops.allow_corrupt_startup = true;
        storage_ = new DiskStorage(1, tmp_dir_, ops);
        auto s = storage_->Open();
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

protected:
    std::string tmp_dir_;
    size_t keep_size_ = std::numeric_limits<size_t>::max();
    DiskStorage* storage_;
};

TEST_F(StorageTest, LogEntry) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99);

    // 读取全部
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 测试term接口
    for (uint64_t i = lo; i < lo; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = storage_->Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - 1]->term());
    }

    // 带maxsize
    ents.clear();
    s = storage_->Entries(lo, hi,
                          to_writes[0]->ByteSizeLong() + to_writes[1]->ByteSizeLong(),
                          &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents2(to_writes.begin(), to_writes.begin() + 2);
    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 至少一条
    ents.clear();
    s = storage_->Entries(lo, hi, 1, &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, std::vector<EntryPtr>{to_writes[0]});
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 读取被截断的日志
    ents.clear();
    s = storage_->Entries(0, hi, std::numeric_limits<uint64_t>::max(), &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(compacted);
    ASSERT_TRUE(ents.empty());

    // 关闭重新打开
    ReOpen();

    // 测试起始index
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99);

    // 读取全部日志
    ents.clear();
    compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 测试term接口
    for (uint64_t i = lo; i < lo; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = storage_->Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - 1]->term());
    }

    // 带maxsize
    ents.clear();
    s = storage_->Entries(lo, hi,
                          to_writes[0]->ByteSizeLong() + to_writes[1]->ByteSizeLong(),
                          &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents3(to_writes.begin(), to_writes.begin() + 2);
    s = Equal(ents, ents3);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Conflict) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto entry = RandomEntry(50, 256);
    s = storage_->StoreEntries(std::vector<EntryPtr>{entry});
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 50);

    // 读取全部
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, 51, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents2(to_writes.begin(), to_writes.begin() + 49);
    ents2.push_back(entry);
    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Snapshot) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    pb::SnapshotMeta meta;
    meta.set_index(sharkstore::randomInt() + 100);
    meta.set_term(sharkstore::randomInt());
    s = storage_->ApplySnapshot(meta);
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(index, meta.index() + 1);
    s = storage_->LastIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(index, meta.index());

    uint64_t term = 0;
    bool compacted = false;
    s = storage_->Term(meta.index() - 20, &term, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(compacted);
    s = storage_->Term(meta.index(), &term, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(term, meta.term());
    ASSERT_FALSE(compacted);

    auto e = RandomEntry(meta.index() + 1);
    s = storage_->StoreEntries(std::vector<EntryPtr>{e});
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::vector<EntryPtr> ents;
    s = storage_->Entries(meta.index() + 1, meta.index() + 2,
                          std::numeric_limits<uint64_t>::max(), &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, std::vector<EntryPtr>{e});
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, KeepCount) {
    SetKeepSize(3);
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    storage_->AppliedTo(99);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto count = storage_->FilesCount();
    auto e = RandomEntry(100);
    s = storage_->StoreEntries(std::vector<EntryPtr>{e});
    auto count2 = storage_->FilesCount();

    std::cout << count << ", " << count2 << std::endl;
    ASSERT_LT(count2, count);
    ASSERT_GE(count2, 3);

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::cout << "First: " << index << std::endl;

    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(index, 101, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);

    ReOpen();
    std::vector<EntryPtr> ents2;
    s = storage_->Entries(index, 101, std::numeric_limits<uint64_t>::max(), &ents2,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);

    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

#ifndef NDEBUG  // debug模式下才开启
TEST_F(StorageTest, Corrupt1) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 最后一个日志文件上添加损坏块
    storage_->TEST_Add_Corruption1();

    // 读取全部
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 重新打开再读取
    ReOpen();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99);

    ents.clear();
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 再写再读
    RandomEntries(hi, hi + 10, 256, &to_writes);
    s = storage_->StoreEntries(
        std::vector<EntryPtr>(to_writes.begin() + hi - 1, to_writes.end()));
    ASSERT_TRUE(s.ok()) << s.ToString();
    hi += 10;
    ents.clear();
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Corrupt2) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 最后一个日志文件上添加损坏块
    storage_->TEST_Add_Corruption2();

    // 重新打开
    ReOpen();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_LT(index, 99);
    ASSERT_GE(index, 1);
    while (to_writes.size() > index) {
        to_writes.pop_back();
    }

    hi = index + 1;
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 再写再读
    RandomEntries(hi, hi + 10, 256, &to_writes);
    s = storage_->StoreEntries(
        std::vector<EntryPtr>(to_writes.begin() + hi - 1, to_writes.end()));
    ASSERT_TRUE(s.ok()) << s.ToString();
    hi += 10;
    ents.clear();
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();
}
#endif

// TEST(Recover, TEST) {
//     auto s1 =
//         new DiskStorage(1, "/home/jonah/36",
//         DiskStorage::Options());
//     auto s = s1 ->Open();
//     ASSERT_TRUE(s.ok()) << s.ToString();

//     auto s2 = new DiskStorage(1, "/home/jonah/67", DiskStorage::Options());
//     s = s2 ->Open();
//     ASSERT_TRUE(s.ok()) << s.ToString();

//     uint64_t first_index = 0;
//     uint64_t last_index = 0;
//     pb::HardState hs;
//     uint64_t term = 0;
//     bool is_compacted = false;

//     s = s1->FirstIndex(&first_index);
//     ASSERT_TRUE(s.ok()) << s.ToString();
//     s = s1->LastIndex(&last_index);
//     ASSERT_TRUE(s.ok()) << s.ToString();
//     std::cout << "36: " << first_index << "-" << last_index << std::endl;
//     s = s1->InitialState(&hs);
//     ASSERT_TRUE(s.ok()) << s.ToString();
//     std::cout << "36: term:" << hs.term() << ", commit:" << hs.commit() << std::endl;

//     s = s1->Term(42570, &term, &is_compacted);
//     std::cout << "36: 42570:" << term << std::endl;

//     std::vector<EntryPtr> ents1;
//     s = s1->Entries(1, 42579, std::numeric_limits<uint64_t>::max(), &ents1,
//     &is_compacted);
//     ASSERT_TRUE(s.ok()) << s.ToString();
//     ASSERT_EQ(ents1.size(), 42578);

//     s = s2->FirstIndex(&first_index);
//     ASSERT_TRUE(s.ok()) << s.ToString();
//     s = s2->LastIndex(&last_index);
//     ASSERT_TRUE(s.ok()) << s.ToString();
//     std::cout << "67: " << first_index << "-" << last_index << std::endl;
//     s = s2->InitialState(&hs);
//     ASSERT_TRUE(s.ok()) << s.ToString();
//     std::cout << "67: term:" << hs.term() << ", commit:" << hs.commit() << std::endl;
//     s = s2->Term(42570, &term, &is_compacted);
//     std::cout << "67: 42570:" << term << std::endl;

//     std::vector<EntryPtr> ents2;
//     s = s2->Entries(1, 42579, std::numeric_limits<uint64_t>::max(), &ents2,
//     &is_compacted);
//     ASSERT_TRUE(s.ok()) << s.ToString();
//     ASSERT_EQ(ents2.size(), 42578);

//     // for (size_t i = 0; i < 42579; ++i) {
//     //     ASSERT_EQ(ents1[i]->index(), i+1) << "wrong s1 index at " << i;
//     //     ASSERT_EQ(ents2[i]->index(), i+1) << "wrong s2 index at " << i;
//     //     EXPECT_EQ(ents1[i]->data(), ents2[i]->data()) << "unequal data(" <<
//     ents1[i]->data() << ", " << ents2[i]->data() << ") at " << i;
//     //     EXPECT_EQ(ents1[i]->term(), ents2[i]->term()) << "unequal term (" <<
//     ents1[i]->term() << ", " << ents2[i]->term() << ") at " << i;
//     // }
//     for (size_t i = 42500; i < 42578; ++i) {
//         std::cout << i+1 << ": " << ents1[i]->term() << "-" << ents2[i]->term() << ","
//             << ents1[i]->type()  << "-" << ents2[i]->type() << std::endl;
//         raft_cmdpb::Command cmd1;
//         ASSERT_TRUE(cmd1.ParseFromString(ents1[i]->data()));
//         std::cout << "cmd1 node: " << cmd1.cmd_id().node_id() << ", seq: " <<
//         cmd1.cmd_id().seq() << ", type: " <<
//             CmdType_Name(cmd1.cmd_type()) << ", size: " << ents1[i]->data().size() <<
//             std::endl;

//         raft_cmdpb::Command cmd2;
//         ASSERT_TRUE(cmd2.ParseFromString(ents2[i]->data()));
//         std::cout << "cmd2 node: " << cmd2.cmd_id().node_id() << ", seq: " <<
//         cmd2.cmd_id().seq() << ", type: " <<
//             CmdType_Name(cmd2.cmd_type()) << ", size: " << ents2[i]->data().size() <<
//             std::endl;
//     }
// }

// TEST(Recover, TEST) {
//    DiskStorage::Options ops;
//    ops.allow_corrupt_startup = false;
//    auto s1 =
//        new DiskStorage(1, "/home/jonah/Downloads/log", ops);
//    auto s = s1 ->Open();
//    ASSERT_TRUE(s.ok()) << s.ToString();
//
//    auto s2 = new DiskStorage(1, "/home/jonah/67", DiskStorage::Options());
//    s = s2 ->Open();
//    ASSERT_TRUE(s.ok()) << s.ToString();
//
//    uint64_t first_index = 0;
//    uint64_t last_index = 0;
//    pb::HardState hs;
//    uint64_t term = 0;
//    bool is_compacted = false;
//
//    s = s1->FirstIndex(&first_index);
//    ASSERT_TRUE(s.ok()) << s.ToString();
//    s = s1->LastIndex(&last_index);
//    ASSERT_TRUE(s.ok()) << s.ToString();
//    std::cout << "36: " << first_index << "-" << last_index << std::endl;
//    s = s1->InitialState(&hs);
//    ASSERT_TRUE(s.ok()) << s.ToString();
//    std::cout << "36: term:" << hs.term() << ", commit:" << hs.commit() << std::endl;
//
//    std::vector<EntryPtr> ents;
//    s = s1->Entries(5, 6, 1024000, &ents, &is_compacted);
//    ASSERT_TRUE(s.ok()) << s.ToString();
//    ASSERT_FALSE(is_compacted);
//
//    std::vector<EntryPtr> ents2{ents[0], ents[0]};
//    s = s1->StoreEntries(ents2);
//    ASSERT_TRUE(s.ok()) << s.ToString();
//}

} /* namespace  */
