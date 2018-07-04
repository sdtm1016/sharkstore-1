_Pragma("once");

#include <stdint.h>
#include <string>
#include "base/status.h"

#include "../raft.pb.h"
#include "../raft_types.h"
#include "log_index.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

class LogIndex;

class LogFile {
public:
    LogFile(const std::string& path, uint64_t seq, uint64_t index);
    virtual ~LogFile();

    LogFile(const LogFile&) = delete;
    LogFile& operator=(const LogFile&) = delete;

    Status Open(bool allow_corrupt, bool last_one = false);
    Status Sync();
    Status Close();
    Status Destroy();

    uint64_t Seq() const { return seq_; }
    uint64_t Index() const { return index_; }
    const std::string& Path() const { return file_path_; }
    uint64_t FileSize() const { return file_size_; }
    int LogSize() const { return log_index_.Size(); }  // 日志条目个数
    uint64_t LastIndex() const { return log_index_.Last(); }

    Status Get(uint64_t index, EntryPtr* e) const;
    Status Term(uint64_t index, uint64_t* term) const;

    Status Append(const EntryPtr& e);
    Status Flush();  // 一次写入的最后一条日志写完需要Flush
    Status Rotate();
    Status Truncate(uint64_t index);

// for tests
#ifndef NDEBUG
    void TEST_Append_RandomData();
    void TEST_Truncate_RandomLen();
#endif

private:
    static std::string makeFilePath(const std::string& path, uint64_t seq,
                                    uint64_t index);

    Status loadIndexes();
    Status traverse(uint32_t& offset);
    Status backup();
    Status recover(bool allow_corrupt);

    Status readFooter(uint32_t* index_ofset) const;
    Status writeFooter(uint32_t index_offset);
    Status readRecord(off_t offset, Record* rec, std::vector<char>* payload) const;
    Status writeRecord(RecordType type, const ::google::protobuf::Message& msg);

private:
    const uint64_t seq_ = 0;    // 日志文件的序号
    const uint64_t index_ = 0;  // 日志文件起始index
    const std::string file_path_;

    int fd_ = -1;
    off_t file_size_ = 0;
    FILE* writer_ = nullptr;
    std::vector<char> write_buf_;

    LogIndex log_index_;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
