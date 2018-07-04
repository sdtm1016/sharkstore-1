#include "meta_file.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstdio>

#include "base/byte_order.h"
#include "base/util.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

MetaFile::MetaFile(const std::string& path) : path_(JoinFilePath({path, "META"})) {}

MetaFile::~MetaFile() { Close(); }

Status MetaFile::Open() {
    fd_ = ::open(path_.c_str(), O_CREAT | O_RDWR, 0644);
    if (-1 == fd_) {
        return Status(Status::kIOError, "open meta file", strErrno(errno));
    }
    return Status::OK();
}

Status MetaFile::Close() {
    if (fd_ > 0) {
        ::close(fd_);
        fd_ = -1;
    }
    return Status::OK();
}

Status MetaFile::Sync() {
    if (::fsync(fd_) == -1) {
        return Status(Status::kIOError, "sync meta file", strErrno(errno));
    }
    return Status::OK();
}

Status MetaFile::Destroy() {
    auto s = Close();
    if (!s.ok()) {
        return s;
    }
    int ret = std::remove(path_.c_str());
    if (ret != 0) {
        return Status(Status::kIOError, "remove meta file", std::to_string(ret));
    } else {
        return Status::OK();
    }
}

Status MetaFile::Load(pb::HardState* hs, pb::TruncateMeta* tm) {
    char buf[kHardStateSize + kTruncateMetaSize] = {'\0'};
    ssize_t ret = ::pread(fd_, buf, kHardStateSize + kTruncateMetaSize, 0);
    if (ret < 0) {
        return Status(Status::kIOError, "load meta", strErrno(errno));
    } else if (ret == 0) {
        return Status::OK();
    } else if (ret == kHardStateSize || ret == kHardStateSize + kTruncateMetaSize) {
        uint64_t term = 0;
        uint64_t commit = 0;
        uint64_t vote = 0;
        memcpy(&term, buf, 8);
        memcpy(&commit, buf + 8, 8);
        memcpy(&vote, buf + 16, 8);
        hs->set_term(be64toh(term));
        hs->set_commit(be64toh(commit));
        hs->set_vote(be64toh(vote));
        if (ret == kHardStateSize + kTruncateMetaSize) {
            uint64_t mindex = 0;
            uint64_t mterm = 0;
            memcpy(&mindex, buf + 24, 8);
            memcpy(&mterm, buf + 32, 8);
            tm->set_index(be64toh(mindex));
            tm->set_term(be64toh(mterm));
        }
        return Status::OK();
    } else {
        return Status(Status::kCorruption, "invalid meta size", std::to_string(ret));
    }
    return Status::OK();
}

Status MetaFile::SaveHardState(const pb::HardState& hs) {
    char buf[kHardStateSize] = {'\0'};
    uint64_t term = htobe64(hs.term());
    uint64_t commit = htobe64(hs.commit());
    uint64_t vote = htobe64(hs.vote());
    memcpy(buf, &term, 8);
    memcpy(buf + 8, &commit, 8);
    memcpy(buf + 16, &vote, 8);

    ssize_t ret = ::pwrite(fd_, buf, kHardStateSize, 0);
    if (ret != kHardStateSize) {
        return Status(Status::kIOError, "write hard state", strErrno(errno));
    }
    return Status::OK();
}

Status MetaFile::SaveTruncMeta(const pb::TruncateMeta& tm) {
    char buf[kTruncateMetaSize] = {'\0'};
    uint64_t index = htobe64(tm.index());
    uint64_t term = htobe64(tm.term());
    memcpy(buf, &index, 8);
    memcpy(buf + 8, &term, 8);

    ssize_t ret = ::pwrite(fd_, buf, kTruncateMetaSize, kHardStateSize);
    if (ret != kTruncateMetaSize) {
        return Status(Status::kIOError, "write trunc meta", strErrno(errno));
    }
    return Status::OK();
}

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
