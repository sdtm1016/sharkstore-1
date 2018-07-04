#include "log_file.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include "base/util.h"

#include "../logger.h"
#include "log_index.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

static const size_t kLogWriteBufSize = 1024 * 16;

LogFile::LogFile(const std::string& path, uint64_t seq, uint64_t index)
    : seq_(seq), index_(index), file_path_(makeFilePath(path, seq, index)) {
    write_buf_.resize(kLogWriteBufSize);
}

LogFile::~LogFile() { Close(); }

std::string LogFile::makeFilePath(const std::string& path, uint64_t seq, uint64_t index) {
    return JoinFilePath({path, makeLogFileName(seq, index)});
}

Status LogFile::Open(bool allow_corrupt, bool last_one) {
    fd_ = ::open(file_path_.c_str(), O_CREAT | O_APPEND | O_RDWR, 0644);
    if (-1 == fd_) {
        return Status(Status::kIOError, "open", strErrno(errno));
    }
    writer_ = ::fdopen(fd_, "a+");
    if (NULL == writer_) {
        return Status(Status::kIOError, "fdopen", strErrno(errno));
    }
    if (::setvbuf(writer_, write_buf_.data(), _IOFBF, write_buf_.size()) != 0) {
        return Status(Status::kIOError, "setvbuf", strErrno(errno));
    }

    // get file size
    struct stat sb;
    memset(&sb, 0, sizeof(sb));
    int ret = fstat(fd_, &sb);
    if (-1 == ret) {
        return Status(Status::kIOError, "stat", strErrno(errno));
    } else {
        file_size_ = sb.st_size;
    }

    if (file_size_ == 0) {  // 新建文件或者空文件
        return Status::OK();
    } else {
        if (!last_one) {
            auto s = loadIndexes();
            if (!s.ok()) {
                return Status(Status::kCorruption,
                              std::string("open log index ") + file_path_, s.ToString());
            }
        } else {
            auto s = recover(allow_corrupt);
            if (!s.ok()) {
                return Status(Status::kCorruption,
                              std::string("recover log file ") + file_path_,
                              s.ToString());
            }
        }
        return Status::OK();
    }
}

Status LogFile::Sync() {
    auto s = Flush();
    if (!s.ok()) {
        return s;
    }

    if (::fsync(fd_) == -1) {
        return Status(Status::kIOError, "sync log file", strErrno(errno));
    } else {
        return Status::OK();
    }
}

Status LogFile::Close() {
    if (fd_ > 0) {
        int ret = ::fclose(writer_);
        if (ret != 0) {
            // TODO: EINTR
            return Status(Status::kIOError, "close", strErrno(errno));
        }
        writer_ = nullptr;
        fd_ = -1;
    }
    return Status::OK();
}

Status LogFile::Destroy() {
    auto s = Close();
    if (!s.ok()) {
        return Status(Status::kIOError, "close", s.ToString());
    }
    int ret = std::remove(file_path_.c_str());
    if (ret != 0) {
        return Status(Status::kIOError, "remove meta file", std::to_string(ret));
    } else {
        return Status::OK();
    }
}

Status LogFile::Get(uint64_t index, EntryPtr* e) const {
    // TODO: check index
    uint32_t offset = log_index_.Offset(index);
    assert(offset < file_size_);
    Record rec;
    std::vector<char> payload;
    auto s = readRecord(offset, &rec, &payload);
    if (!s.ok()) return s;
    if (rec.type != RecordType::kLogEntry) {
        return Status(Status::kCorruption, "read log entry", "invalid record type");
    }

    EntryPtr entry(new impl::pb::Entry);
    // TODO: check crc
    if (!entry->ParseFromArray(payload.data(), payload.size())) {
        return Status(Status::kCorruption, "read log entry", "deserizial failed");
    }
    if (entry->index() != index) {
        return Status(Status::kCorruption, "inconsisent entry index",
                      std::to_string(entry->index()));
    }
    *e = entry;
    return Status::OK();
}

Status LogFile::Term(uint64_t index, uint64_t* term) const {
    // TODO: check index
    *term = log_index_.Term(index);
    return Status::OK();
}

Status LogFile::Append(const EntryPtr& e) {
    if (log_index_.Empty()
            ? (e->index() != index_)
            : (e->index() < log_index_.First() || e->index() > log_index_.Last() + 1)) {
        return Status(Status::kInvalidArgument,
                      std::string("append log index(") + std::to_string(e->index()) +
                          ") out of bound",
                      std::to_string(index_) + "-[" + std::to_string(log_index_.First()) +
                          "," + std::to_string(log_index_.Last()) + "]");
    }
    // 冲突覆盖写，先截断
    if (e->index() <= log_index_.Last()) {
        auto s = this->Truncate(e->index());
        if (!s.ok()) {
            return s;
        }
    }
    uint32_t offset = file_size_;
    auto s = writeRecord(RecordType::kLogEntry, *e);
    if (!s.ok()) {
        return s;
    } else {
        // 更新索引
        log_index_.Append(e->index(), e->term(), offset);
        return Status::OK();
    }
}

Status LogFile::Flush() {
    int ret = ::fflush(writer_);
    if (ret == 0) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "fflush", strErrno(errno));
    }
}

Status LogFile::Rotate() {
    uint32_t offset = file_size_;
    std::vector<char> buf;
    pb::LogIndex pb_index;
    log_index_.Serialize(&pb_index);
    auto s = writeRecord(RecordType::kIndex, pb_index);
    if (!s.ok()) {
        return s;
    }
    s = writeFooter(offset);
    if (!s.ok()) {
        return s;
    }
    return Sync();
}

Status LogFile::loadIndexes() {
    // 读取索引offset
    uint32_t index_offset;
    auto s = readFooter(&index_offset);
    if (!s.ok()) return s;

    // 读索引数据
    Record rec;
    std::vector<char> payload;
    s = readRecord(index_offset, &rec, &payload);
    if (!s.ok()) {
        return Status(Status::kCorruption, "read log index",
                      std::to_string(index_offset));
    }
    // 解析索引数据
    s = log_index_.ParseFrom(rec, payload);
    if (!s.ok()) {
        return s;
    }
    // 检查索引数据跟文件的起始index是否一致
    if (log_index_.First() != index_) {
        return Status(
            Status::kCorruption, "inconsistent log first index",
            std::to_string(log_index_.First()) + " != " + std::to_string(index_));
    }

    return Status::OK();
};

Status LogFile::traverse(uint32_t& offset) {
    Status s;
    while (offset < static_cast<uint64_t>(file_size_)) {
        Record rec;
        std::vector<char> payload;
        s = readRecord(offset, &rec, &payload);
        if (s.code() == Status::kEndofFile) {
            return Status::OK();
        } else if (!s.ok()) {
            return Status(Status::kCorruption,
                          "read record at offset " + std::to_string(offset),
                          s.ToString());
        }
        if (rec.type == RecordType::kLogEntry) {
            impl::pb::Entry e;
            if (!e.ParseFromArray(payload.data(), payload.size())) {
                return Status(Status::kCorruption,
                              "parse entry at offset " + std::to_string(offset),
                              "pb return false");
            }
            bool valid = log_index_.Empty() ? (index_ == e.index())
                                            : (log_index_.Last() + 1 == e.index());
            if (!valid) {
                return Status(Status::kCorruption,
                              std::string("invalid log entry index ") +
                                  std::to_string(e.index()) + ", prev is " +
                                  std::to_string(log_index_.Last()),
                              std::to_string(offset));
            } else {
                log_index_.Append(e.index(), e.term(), offset);
            }
        } else if (rec.type == RecordType::kIndex) {
            log_index_.Clear();
            auto s = loadIndexes();
            if (s.ok()) {
                // TODO: 可以load的跟遍历的index作个对比
                return Status::OK();
            } else {
                return Status(Status::kCorruption,
                              std::string("load indexes failed. last offset") +
                                  std::to_string(offset),
                              s.ToString());
            }
        } else {
            return Status(
                Status::kCorruption,
                std::string("invalid record type at offset") + std::to_string(offset),
                std::to_string(rec.type));
        }
        offset += (sizeof(Record) + payload.size());
    }
    return Status::OK();
}

Status LogFile::backup() {
    std::string bak_path = file_path_ + ".bak." + std::to_string(time(NULL));
    try {
        std::ifstream source(file_path_, std::ios::binary);
        std::ofstream dest(bak_path, std::ios::binary);
        dest << source.rdbuf();
        source.close();
        dest.close();
    } catch (std::exception& e) {
        return Status(Status::kIOError,
                      std::string("backup log file ") + file_path_ + " failed", e.what());
    }
    return Status::OK();
}

Status LogFile::recover(bool allow_corrupt) {
    uint32_t offset = 0;
    auto s = traverse(offset);
    if (!s.ok()) {
        if (!allow_corrupt) {
            return s;
        } else {
            s = backup();
            if (!s.ok()) {
                return s;
            }
            int ret = ::ftruncate(fd_, offset);
            if (-1 == ret) {
                return Status(Status::kIOError, "truncate log file", strErrno(errno));
            }
            file_size_ = offset;
            LOG_WARN("[raft log] truncate(offset: %d) and backup corrupt log: %s", offset,
                     file_path_.c_str(), offset);
        }
    }
    return Status::OK();
}

Status LogFile::readFooter(uint32_t* index_offset) const {
    if (file_size_ < static_cast<int64_t>(sizeof(Footer))) {
        return Status(Status::kCorruption, "insufficient log file size",
                      std::to_string(file_size_));
    }

    // 读取footer
    Footer footer;
    memset(&footer, 0, sizeof(footer));
    int ret = ::pread(fd_, &footer, sizeof(footer), file_size_ - sizeof(footer));
    if (ret == -1) {
        return Status(Status::kIOError, "read log footer", strErrno(errno));
    } else if (ret < static_cast<int64_t>(sizeof(footer))) {
        return Status(Status::kCorruption, "insufficient log file size",
                      std::to_string(file_size_));
    }
    footer.Decode();
    auto s = footer.Validate();
    if (!s.ok()) return s;

    // 检查index offset是否有效
    if (footer.index_offset >= file_size_ - sizeof(footer)) {
        return Status(Status::kCorruption, "invalid footer index offset",
                      std::to_string(footer.index_offset));
    }

    *index_offset = footer.index_offset;
    return Status::OK();
}

Status LogFile::writeFooter(uint32_t index_offset) {
    Footer footer;
    strncpy(footer.magic, kLogFileMagic, strlen(kLogFileMagic));
    footer.index_offset = index_offset;
    footer.Encode();

    int ret = ::fwrite(&footer, sizeof(footer), 1, writer_);
    if (ret != 1) {
        return Status(Status::kIOError, "write footer", strErrno(errno));
    }

    file_size_ += sizeof(footer);

    return Status::OK();
}

Status LogFile::readRecord(off_t offset, Record* rec, std::vector<char>* payload) const {
    // 读记录头
    memset(rec, 0, sizeof(Record));
    int ret = ::pread(fd_, rec, sizeof(Record), offset);
    if (ret == -1) {
        return Status(Status::kIOError, "read log record", strErrno(errno));
    } else if (ret == 0) {
        return Status(Status::kEndofFile, "read log record", strErrno(errno));
    } else if (ret < static_cast<int64_t>(sizeof(Record))) {
        return Status(Status::kCorruption, "insufficient log record size",
                      std::to_string(ret));
    }
    rec->Decode();

    // 检查payload的大小有没超过文件末尾
    if (offset + sizeof(Record) + rec->size > static_cast<uint64_t>(file_size_)) {
        return Status(Status::kCorruption, "log size too large",
                      std::to_string(rec->size));
    }

    // 读payload数据
    payload->resize(rec->size);
    ret = ::pread(fd_, payload->data(), rec->size, offset + sizeof(Record));
    if (ret == -1) {
        return Status(Status::kIOError, "read log record payload", strErrno(errno));
    } else if (static_cast<uint32_t>(ret) < rec->size) {
        return Status(Status::kCorruption, "insufficient log record payload size",
                      std::to_string(ret));
    }

    return Status::OK();
}

Status LogFile::writeRecord(RecordType type, const ::google::protobuf::Message& msg) {
    uint32_t size = static_cast<uint32_t>(msg.ByteSizeLong());
    std::vector<char> buf;
    buf.resize(size + sizeof(Record));
    Record* rec = (Record*)(buf.data());
    rec->type = type;
    rec->crc = 0;
    rec->size = size;
    rec->Encode();
    if (!msg.SerializeToArray(rec->payload, size)) {
        return Status(Status::kCorruption, "serialize log record", "pb return false");
    }

    int ret = ::fwrite(buf.data(), buf.size(), 1, writer_);
    if (ret != 1) {
        return Status(Status::kIOError, "write record", strErrno(errno));
    }

    file_size_ += buf.size();

    return Status::OK();
}

Status LogFile::Truncate(uint64_t index) {
    if (log_index_.Empty() || log_index_.Last() < index) {
        return Status::OK();
    }
    uint32_t offset = log_index_.Offset(index);
    assert(offset < file_size_);
    int ret = ::ftruncate(fd_, offset);
    if (ret == -1) {
        return Status(Status::kIOError, "truncate log", strErrno(errno));
    } else {
        log_index_.Truncate(index);
        file_size_ = offset;
        return Status::OK();
    }
}

#ifndef NDEBUG
void LogFile::TEST_Append_RandomData() {
    std::string data = randomString(10);
    int ret = ::write(fd_, data.data(), data.length());
    assert(ret == static_cast<int>(data.length()));
    file_size_ += data.size();
}

void LogFile::TEST_Truncate_RandomLen() {
    if (file_size_ > 0) {
        int offset = randomInt() % file_size_;
        int ret = ::ftruncate(fd_, offset);
        assert(ret == 0);
        file_size_ = offset;
    }
}

#endif

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
