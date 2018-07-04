_Pragma("once");

#include "meta_file.h"
#include "storage.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

class LogFile;

class DiskStorage : public Storage {
public:
    struct Options {
        // 一个日志文件的大小
        size_t log_file_size = 1024 * 1024 * 16;

        // 最多保留多少个日志文件，超过此数则截断旧文件
        size_t max_log_files = std::numeric_limits<size_t>::max();

        // 启动时检测到文件损坏是否继续，若是则备份可以正常打开工作
        bool allow_corrupt_startup = false;

        // 只读模式
        bool readonly = false;
    };

    DiskStorage(uint64_t id, const std::string& path, const Options& ops);
    ~DiskStorage();

    DiskStorage(const DiskStorage&) = delete;
    DiskStorage& operator=(const DiskStorage&) = delete;

    Status Open() override;

    Status StoreHardState(const pb::HardState& hs) override;
    Status InitialState(pb::HardState* hs) const override;

    Status StoreEntries(const std::vector<EntryPtr>& entries) override;
    Status Term(uint64_t index, uint64_t* term, bool* is_compacted) const override;
    Status FirstIndex(uint64_t* index) const override;
    Status LastIndex(uint64_t* index) const override;
    Status Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                   std::vector<EntryPtr>* entries, bool* is_compacted) const override;

    Status Truncate(uint64_t index) override;

    Status ApplySnapshot(const pb::SnapshotMeta& meta) override;

    void AppliedTo(uint64_t applied) override;

    Status Close() override;
    Status Destroy() override;
    Status Backup() override;

    size_t FilesCount() const { return log_files_.size(); }

// for tests
#ifndef NDEBUG
    void TEST_Add_Corruption1();
    void TEST_Add_Corruption2();
    void TEST_Add_Corruption3();
#endif

    Status CheckCorrupt();

private:
    static Status checkLogsValidate(const std::map<uint64_t, uint64_t>& logs);

    Status initDir();
    Status listLogs(std::map<uint64_t, uint64_t>* logs);
    Status openLogs();
    Status closeLogs();
    Status removeBakups();  // 删除备份的日志文件

    // 截断旧日志
    Status truncateOld(uint64_t index);
    // 截断最新的日志
    Status truncateNew(uint64_t index);
    // 清空日志（应用快照时）
    Status truncateAll();

    Status tryRotate();
    Status save(const EntryPtr& e);
    LogFile* locate(uint64_t index) const;
    Status load(uint64_t index, EntryPtr* e) const;

private:
    const uint64_t id_ = 0;
    const std::string path_;
    const Options ops_;

    MetaFile meta_file_;
    pb::HardState hard_state_;
    pb::TruncateMeta trunc_meta_;
    uint64_t applied_ = 0;  // 大于applied_的不可截断

    std::vector<LogFile*> log_files_;
    uint64_t next_file_seq_ = 1;
    uint64_t last_index_ = 0;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
