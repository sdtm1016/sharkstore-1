#include "range_context_mock.h"

#include <unistd.h>

#include "base/util.h"
#include "storage/meta_store.h"
#include "range/split_policy.h"
#include "range/range.h"

#include "master_worker_mock.h"
#include "raft_server_mock.h"
#include "socket_session_mock.h"

namespace sharkstore {
namespace test {
namespace mock {

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::range;


Status RangeContextMock::Init() {
    // init db
    char path[] = "/tmp/sharkstore_ds_range_mock_XXXXXX";
    char* tmp = mkdtemp(path);
    if (tmp == NULL) {
        return Status(Status::kIOError, "mkdtemp", "");
    }
    // open rocksdb
    path_ = path;
    rocksdb::Options ops;
    ops.create_if_missing = true;
    auto s = rocksdb::DB::Open(ops, JoinFilePath({path_, "data"}), &db_);
    if (!s.ok()) {
        return Status(Status::kIOError, "open rocksdb", s.ToString());
    }
    // open meta db
    meta_store_.reset(new storage::MetaStore(JoinFilePath({path_, "meta"})));
    auto ret = meta_store_->Open();
    if (!ret.ok()) return ret;

    // master worker
    master_worker_.reset(new MasterWorkerMock);

    // raft_server
    raft_server_.reset(new RaftServerMock);

    // socket session
    socket_session_.reset(new SocketSessionMock);

    // range stats
    range_stats_.reset(new RangeStats);

    // split policy
    split_policy_ = NewDisableSplitPolicy();

    // watch server
    watch_server_.reset(new watch::WatchServer);

    return Status::OK();
}

void RangeContextMock::Destroy() {
    if (!path_.empty()) {
        RemoveDirAll(path_.c_str());
    }
}


void RangeContextMock::ScheduleHeartbeat(uint64_t range_id, bool delay) {
}

void RangeContextMock::ScheduleCheckSize(uint64_t range_id) {

}

Status RangeContextMock::CreateRange(const metapb::Range& meta, uint64_t leader,
                   uint64_t index, std::shared_ptr<Range> *result) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = ranges_.find(meta.id());
    if (it != ranges_.end()) {
        return Status(Status::kExisted);
    }
    auto rng = std::make_shared<Range>(this, meta);
    auto s = rng->Initialize(leader, index);
    if (!s.ok()) {
        return s;
    }
    ranges_.emplace(meta.id(), rng);
    if (result) {
        *result = rng;
    }
    return Status::OK();
}

std::shared_ptr<Range> RangeContextMock::FindRange(uint64_t range_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = ranges_.find(range_id);
    if (it == ranges_.end()) {
        return nullptr;
    } else {
        return it->second;
    }
}

Status RangeContextMock::SplitRange(uint64_t range_id, const raft_cmdpb::SplitRequest &req, uint64_t raft_index) {
    CreateRange(req.new_range(), req.leader(), raft_index);
    return Status::OK();
}

}
}
}
