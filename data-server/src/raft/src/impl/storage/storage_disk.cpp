#include "storage_disk.h"

#include <assert.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <sstream>

#include "../logger.h"
#include "base/util.h"
#include "log_file.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

// 只截断已应用的减去kKeepCountBeforeApplied之前的日志
static const unsigned kKeepLogCountBeforeApplied = 30;

DiskStorage::DiskStorage(uint64_t id, const std::string& path, const Options& ops)
    : id_(id), path_(path), ops_(ops), meta_file_(path) {}

DiskStorage::~DiskStorage() { Close(); }

Status DiskStorage::Open() {
    // 初始化目录，不存在则创建
    auto s = initDir();
    if (!s.ok()) {
        return s;
    }

    // 打开meta file
    s = meta_file_.Open();
    if (!s.ok()) {
        return s;
    }
    s = meta_file_.Load(&hard_state_, &trunc_meta_);
    if (!s.ok()) {
        return s;
    }
    applied_ = hard_state_.commit();

    // 打开日志文件
    s = openLogs();
    if (!s.ok()) {
        return s;
    }

    // 检查meta和日志文件是否一致
    assert(!log_files_.empty());
    uint64_t first = log_files_[0]->Index();
    if (trunc_meta_.index() + 1 < first) {  // 中间有间隔
        return Status(
            Status::kCorruption, "inconsistent truncate meta with log files",
            std::to_string(trunc_meta_.index() + 1) + " < " + std::to_string(first));
    }

    return Status::OK();
}

Status DiskStorage::checkLogsValidate(const std::map<uint64_t, uint64_t>& logs) {
    // 检查日志文件的序号是否连续
    // 检查日志文件的起始index是否有序
    uint64_t prev_seq = 0;
    uint64_t prev_index = 0;
    for (std::map<uint64_t, uint64_t>::const_iterator it = logs.cbegin();
         it != logs.cend(); ++it) {
        if (it != logs.cbegin()) {
            if (prev_seq + 1 != it->first || prev_index >= it->second) {
                std::ostringstream ss;
                ss << "invalid log file order between (" << prev_seq << "-" << prev_index
                   << ") and (" << it->first << "-" << it->second << ")";
                return Status(Status::kCorruption, "raft logger", ss.str());
            }
        }
        prev_seq = it->first;
        prev_index = it->second;
    }
    return Status::OK();
}

Status DiskStorage::initDir() {
    assert(!path_.empty());
    int ret = sharkstore::MakeDirAll(path_, 0755);
    if (ret < 0) {
        return Status(Status::kIOError, "init directory " + path_, strErrno(errno));
    }
    return Status::OK();
}

Status DiskStorage::listLogs(std::map<uint64_t, uint64_t>* logs) {
    logs->clear();

    DIR* dir = ::opendir(path_.c_str());
    if (NULL == dir) {
        return Status(Status::kIOError, "call opendir", strErrno(errno));
    }

    struct dirent* ent = NULL;
    while (true) {
        errno = 0;
        ent = ::readdir(dir);
        if (NULL == ent) {
            if (0 == errno) {
                break;
            } else {
                closedir(dir);
                return Status(Status::kIOError, "call readdir", strErrno(errno));
            }
        }
        // TODO: call stat if d_type is DT_UNKNOWN
        if (ent->d_type == DT_REG || ent->d_type == DT_UNKNOWN) {
            uint64_t seq = 0;
            uint64_t offset = 0;
            if (!parseLogFileName(ent->d_name, seq, offset)) {
                continue;
            }
            auto it = logs->emplace(seq, offset);
            if (!it.second) {
                closedir(dir);
                return Status(Status::kIOError, "repeated log sequence",
                              std::to_string(seq));
            }
        }
    }
    closedir(dir);
    return Status::OK();
}

Status DiskStorage::openLogs() {
    std::map<uint64_t, uint64_t> logs;
    auto s = listLogs(&logs);
    if (!s.ok()) return s;
    s = checkLogsValidate(logs);
    if (!s.ok()) return s;

    if (logs.empty()) {
        LogFile* f = new LogFile(path_, 1, trunc_meta_.index() + 1);
        auto s = f->Open(ops_.allow_corrupt_startup);
        if (!s.ok()) {
            return s;
        }
        log_files_.push_back(f);
        next_file_seq_ = 2;
    } else {
        size_t count = 0;
        for (auto it = logs.begin(); it != logs.end(); ++it) {
            LogFile* f = new LogFile(path_, it->first, it->second);
            auto s = f->Open(ops_.allow_corrupt_startup, count == logs.size() - 1);
            if (!s.ok()) {
                return s;
            } else {
                log_files_.push_back(f);
            }
            ++count;
        }
        next_file_seq_ = log_files_.back()->Seq() + 1;
    }

    // 恢复last index
    auto last = log_files_.back();
    last_index_ = (last->LogSize() == 0) ? (last->Index() - 1) : last->LastIndex();

    return Status::OK();
}

Status DiskStorage::closeLogs() {
    std::for_each(log_files_.begin(), log_files_.end(), [](LogFile* f) { delete f; });
    log_files_.clear();
    return Status::OK();
}

Status DiskStorage::StoreHardState(const pb::HardState& hs) {
    // 持久化
    auto s = meta_file_.SaveHardState(hs);
    if (!s.ok()) return s;
    // 更新内存
    hard_state_ = hs;
    // TODO: sync?
    return Status::OK();
}

Status DiskStorage::InitialState(pb::HardState* hs) const {
    *hs = hard_state_;
    return Status::OK();
}

Status DiskStorage::tryRotate() {
    auto f = log_files_.back();
    if (f->FileSize() >= ops_.log_file_size) {
        auto s = f->Rotate();
        if (!s.ok()) {
            return s;
        }
        auto newf = new LogFile(path_, next_file_seq_, last_index_ + 1);
        s = newf->Open(false);
        if (!s.ok()) {
            return s;
        }
        log_files_.push_back(newf);
        ++next_file_seq_;
    }
    return Status::OK();
}

Status DiskStorage::save(const EntryPtr& e) {
    auto s = tryRotate();
    if (!s.ok()) return s;
    auto f = log_files_.back();
    s = f->Append(e);
    if (!s.ok()) {
        return s;
    }
    last_index_ = e->index();
    return Status::OK();
}

LogFile* DiskStorage::locate(uint64_t index) const {
    // TODO: binary search
    auto it = std::find_if(log_files_.rbegin(), log_files_.rend(),
                           [index](LogFile* f) { return f->Index() <= index; });
    return *it;
}

Status DiskStorage::load(uint64_t index, EntryPtr* e) const {
    return locate(index)->Get(index, e);
}

Status DiskStorage::StoreEntries(const std::vector<EntryPtr>& entries) {
    Status s;

    // 检测日志文件个数是否太多需要截断
    if (ops_.max_log_files > 0 && log_files_.size() > ops_.max_log_files &&
        applied_ > kKeepLogCountBeforeApplied) {
        for (int idx = log_files_.size() - ops_.max_log_files - 1; idx >= 0; --idx) {
            // 只截断已经applied的日志
            if (log_files_[idx]->LastIndex() < applied_ - kKeepLogCountBeforeApplied) {
                s = Truncate(log_files_[idx]->LastIndex());
                if (!s.ok()) {
                    return Status(Status::kIOError, "truncate log file", s.ToString());
                }
                break;
            }
        }
    }

    if (entries.empty()) return Status::OK();

    // 检查参数的index是否是递增加1的
    for (size_t i = 1; i < entries.size(); ++i) {
        if (entries[i]->index() != entries[i - 1]->index() + 1) {
            return Status(Status::kInvalidArgument, "StoreEntries",
                          std::string("discontinuous index between ") +
                              std::to_string(entries[i]->index()) + " and " +
                              std::to_string(entries[i - 1]->index()) + " at " +
                              std::to_string(i - 1));
        }
    }

    if (entries[0]->index() > last_index_ + 1) {  // 不连续
        return Status(Status::kInvalidArgument,
                      std::string("append log index(") +
                          std::to_string(entries[0]->index()) + ") out of bound",
                      std::string("current last is ") + std::to_string(last_index_));
    } else if (entries[0]->index() <= last_index_) {
        // 有冲突
        s = truncateNew(entries[0]->index());
        if (!s.ok()) {
            return s;
        }
    }

    for (const auto& e : entries) {
        s = save(e);
        if (!s.ok()) {
            return s;
        }
    }
    s = log_files_.back()->Flush();
    if (!s.ok()) {
        return s;
    }
    return Status::OK();
}

Status DiskStorage::Term(uint64_t index, uint64_t* term, bool* is_compacted) const {
    if (index < trunc_meta_.index()) {
        *term = 0;
        *is_compacted = true;
        return Status::OK();
    } else if (index == trunc_meta_.index()) {
        *term = trunc_meta_.term();
        *is_compacted = false;
        return Status::OK();
    } else if (index > last_index_) {
        return Status(Status::kInvalidArgument, "out of bound", std::to_string(index));
    } else {
        *is_compacted = false;
        return locate(index)->Term(index, term);
    }
}

Status DiskStorage::FirstIndex(uint64_t* index) const {
    *index = trunc_meta_.index() + 1;
    return Status::OK();
}

Status DiskStorage::LastIndex(uint64_t* index) const {
    *index = std::max(last_index_, trunc_meta_.index());
    return Status::OK();
}

Status DiskStorage::Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                            std::vector<EntryPtr>* entries, bool* is_compacted) const {
    if (lo <= trunc_meta_.index()) {
        *is_compacted = true;
        return Status::OK();
    } else if (hi > last_index_ + 1) {
        return Status(Status::kInvalidArgument, "out of bound", std::to_string(hi));
    }

    uint64_t size = 0;
    Status s;
    for (uint64_t index = lo; index < hi; ++index) {
        EntryPtr e;
        // TODO: 定位文件只需要一次
        s = load(index, &e);
        if (!s.ok()) return s;
        size += e->ByteSizeLong();
        if (size > max_size) {
            if (entries->empty()) {  // 至少一条
                entries->push_back(e);
            }
            break;
        } else {
            entries->push_back(e);
        }
    }
    return Status::OK();
}

Status DiskStorage::truncateOld(uint64_t index) {
    while (log_files_.size() > 1) {
        auto f = log_files_[0];
        if (f->LastIndex() <= index) {
            auto s = f->Destroy();
            if (!s.ok()) return s;
            delete f;
            log_files_.erase(log_files_.begin());
        } else {
            break;
        }
    }
    return Status::OK();
}

Status DiskStorage::truncateNew(uint64_t index) {
    // 截断冲突
    Status s;
    while (!log_files_.empty()) {
        auto last = log_files_.back();
        if (last->Index() > index) {
            s = last->Destroy();
            if (!s.ok()) return s;
            delete last;
            log_files_.pop_back();
        } else {
            s = last->Truncate(index);
            if (!s.ok()) {
                return s;
            } else {
                last_index_ = index - 1;
                return Status::OK();
            }
        }
    }

    if (log_files_.empty()) {
        return Status(Status::kInvalidArgument, "append log index less than truncated",
                      std::to_string(index));
    }
    return Status::OK();
}

// 清空日志（应用快照时）
Status DiskStorage::truncateAll() {
    Status s;
    for (auto it = log_files_.begin(); it != log_files_.end(); ++it) {
        s = (*it)->Destroy();
        if (!s.ok()) {
            return s;
        }
        delete (*it);
    }
    log_files_.clear();

    LogFile* f = new LogFile(path_, 1, trunc_meta_.index() + 1);
    s = f->Open(false);
    if (!s.ok()) {
        return s;
    }
    log_files_.push_back(f);
    next_file_seq_ = 2;
    last_index_ = trunc_meta_.index();

    return Status::OK();
}

Status DiskStorage::Truncate(uint64_t index) {
    // 未被应用的，不能截断
    if (index > applied_) {
        return Status(Status::kInvalidArgument, "try to truncate not applied logs",
                      std::to_string(index) + " > " + std::to_string(applied_));
    }
    // 已经截断
    if (index <= trunc_meta_.index()) {
        return Status::OK();
    }

    // 获取truncate index对应的term
    uint64_t term = 0;
    bool is_compacted = false;
    auto s = Term(index, &term, &is_compacted);
    if (!s.ok()) {
        return s;
    } else if (is_compacted) {
        return Status(Status::kCorruption, "truncate term is compacted",
                      std::to_string(index));
    }

    // 更新内存中的truncate_mate并持久化
    trunc_meta_.set_index(index);
    trunc_meta_.set_term(term);
    s = meta_file_.SaveTruncMeta(trunc_meta_);
    if (!s.ok()) {
        return s;
    }
    s = meta_file_.Sync();
    if (!s.ok()) {
        return s;
    }

    // 截断旧日志
    s = truncateOld(index);
    if (s.ok()) {
        return s;
    }

    LOG_INFO("raftlog[%lu] truncate to %lu", id_, index);

    return Status::OK();
}

Status DiskStorage::ApplySnapshot(const pb::SnapshotMeta& meta) {
    // 更新持久化HardState
    hard_state_.set_commit(meta.index());
    auto s = meta_file_.SaveHardState(hard_state_);
    if (!s.ok()) {
        return s;
    }

    // 更新并持久化truncate meta
    trunc_meta_.set_index(meta.index());
    trunc_meta_.set_term(meta.term());
    s = meta_file_.SaveTruncMeta(trunc_meta_);
    if (!s.ok()) {
        return s;
    }

    // sync meta file
    s = meta_file_.Sync();
    if (!s.ok()) {
        return s;
    }

    // 清空日志
    return truncateAll();
}

void DiskStorage::AppliedTo(uint64_t applied) {
    if (applied > applied_) {
        applied_ = applied;
    }
}

Status DiskStorage::Close() {
    auto s = meta_file_.Close();
    if (!s.ok()) return s;
    return closeLogs();
}

Status DiskStorage::removeBakups() {
    DIR* dir = ::opendir(path_.c_str());
    if (NULL == dir) {
        return Status(Status::kIOError, "remove backups: call opendir", strErrno(errno));
    }
    struct dirent* ent = NULL;
    while (true) {
        errno = 0;
        ent = ::readdir(dir);
        if (NULL == ent) {
            if (0 == errno) {
                break;
            } else {
                closedir(dir);
                return Status(Status::kIOError, "call readdir", strErrno(errno));
            }
        }
        // TODO: call stat if d_type is DT_UNKNOWN
        if ((ent->d_type == DT_REG || ent->d_type == DT_UNKNOWN) &&
            isBakLogFile(ent->d_name)) {
            std::string bak_path = JoinFilePath({path_, ent->d_name});
            if (std::remove(bak_path.c_str()) != 0) {
                closedir(dir);
                return Status(Status::kIOError,
                              std::string("remove log backup ") + bak_path,
                              strErrno(errno));
            }
        }
    }
    closedir(dir);
    return Status::OK();
}

Status DiskStorage::Destroy() {
    // 删除日志文件
    Status s;
    for (auto f : log_files_) {
        s = f->Destroy();
        if (!s.ok()) {
            return s;
        }
    }
    // 删除日志备份
    s = removeBakups();
    if (!s.ok()) {
        return s;
    }

    // 删除meta文件
    s = meta_file_.Destroy();
    if (!s.ok()) {
        return s;
    }

    // 删除目录
    int ret = std::remove(path_.c_str());
    if (ret != 0) {
        return Status(Status::kIOError, "remove log dir", strErrno(errno));
    }
    return s;
}

Status DiskStorage::Backup() {
    // TODO: implement this
    return Status(Status::kNotSupported, "raft log backup", "");
}

#ifndef NDEBUG
void DiskStorage::TEST_Add_Corruption1() {
    //
    log_files_.back()->TEST_Append_RandomData();
}

void DiskStorage::TEST_Add_Corruption2() {
    //
    log_files_.back()->TEST_Truncate_RandomLen();
}

void DiskStorage::TEST_Add_Corruption3() {
    // TODO:
}
#endif

Status DiskStorage::CheckCorrupt() {
    // TODO:
    return Status(Status::kNotSupported);
}

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
