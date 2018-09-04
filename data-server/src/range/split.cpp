#include "range.h"

#include <sstream>
#include "frame/sf_util.h"
#include "master/worker.h"
#include "server/range_server.h"
#include "server/server.h"
#include "base/util.h"

#include "stats.h"
#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

void Range::CheckSplit(uint64_t size) {
    statis_size_ += size;

    // split disabled
    if (!context_->GetSplitPolicy()->IsEnabled()) {
        return;
    }

    auto check_size = context_->GetSplitPolicy()->CheckSize();
    if (!statis_flag_ && statis_size_ > check_size) {
        statis_flag_ = true;
        context_->ScheduleCheckSize(id_);
    }
}

void Range::ResetStatisSize() {
    // split size is split size, not half of split size
    // amicable sequence writing and random writing
    // const static uint64_t split_size = ds_config.range_config.split_size >>
    // 1;
    auto policy = context_->GetSplitPolicy();
    auto meta = meta_.Get();

    uint64_t total_size = 0;
    std::string split_key;
    auto s = store_->StatSize(policy->SplitSize(), policy->KeyMode(),
            &total_size , &split_key);
    statis_flag_ = false;
    statis_size_ = 0;
    if (!s.ok()) {
        if (s.code() == Status::kUnexpected) {
            RANGE_LOG_INFO("StatSize failed: %s", s.ToString().c_str());
        } else {
            RANGE_LOG_ERROR("StatSize failed: %s", s.ToString().c_str());
        }
        return;
    }
    real_size_ = total_size;
    if (split_key <= meta.start_key() || split_key >= meta.end_key()) {
        RANGE_LOG_ERROR("StatSize invalid split key: %s vs scope[%s-%s]",
                EncodeToHex(split_key).c_str(),
                EncodeToHex(meta.start_key()).c_str(),
                EncodeToHex(meta.end_key()).c_str());
        return ;
    }

    RANGE_LOG_INFO("StatSize policy: %s, real size: %" PRIu64 ", split key: %s",
            policy->Description().c_str(), real_size_, EncodeToHex(split_key).c_str());

    if (!EpochIsEqual(meta.range_epoch())) {
        RANGE_LOG_WARN("StatSize epoch is changed");
        return;
    }

    // when real size >= max size, we need split with split size
    if (real_size_ >= context_->GetSplitPolicy()->MaxSize()) {
        return AskSplit(std::move(split_key), std::move(meta));
    }
}

void Range::AskSplit(std::string &&key, metapb::Range&& meta, bool force) {
    assert(!key.empty());
    assert(key >= meta.start_key());
    assert(key < meta.end_key());

    RANGE_LOG_INFO("AskSplit, version: %" PRIu64 ", key: %s, policy: %s",
            meta.range_epoch().version(), EncodeToHex(key).c_str(),
            context_->GetSplitPolicy()->Description().c_str());

    mspb::AskSplitRequest ask;
    ask.set_allocated_range(new metapb::Range(std::move(meta)));
    ask.set_split_key(std::move(key));
    ask.set_force(force);
    context_->MasterClient()->AsyncAskSplit(ask);
}

void Range::ReportSplit(const metapb::Range &new_range) {
    mspb::ReportSplitRequest report;
    meta_.Get(report.mutable_left());
    report.set_allocated_right(new metapb::Range(new_range));
    context_->MasterClient()->AsyncReportSplit(report);
}

void Range::AdminSplit(mspb::AskSplitResponse &resp) {
    if (!EpochIsEqual(resp.range().range_epoch())) {
        RANGE_LOG_WARN("AdminSplit epoch is changed");
        return;
    }

    auto &split_key = resp.split_key();

    RANGE_LOG_INFO(
        "AdminSplit new_range_id: %" PRIu64 " split_key: %s",
        resp.new_range_id(), EncodeToHex(split_key).c_str());

    raft_cmdpb::Command cmd;
    // set cmd_id
    cmd.mutable_cmd_id()->set_node_id(node_id_);
    cmd.mutable_cmd_id()->set_seq(submit_queue_.GetSeq());
    // set cmd_type
    cmd.set_cmd_type(raft_cmdpb::CmdType::AdminSplit);

    // set cmd admin_split_req
    auto split_req = cmd.mutable_admin_split_req();

    split_req->set_split_key(split_key);

    auto range = resp.release_range();

    auto psize = range->peers_size();
    auto rsize = resp.new_peer_ids_size();
    if (psize != rsize) {
        RANGE_LOG_WARN("AdminSplit peers_size no equal");
        return;
    }

    auto epoch = range->mutable_range_epoch();

    // set verify epoch
    cmd.set_allocated_verify_epoch(new metapb::RangeEpoch(*epoch));

    // set leader
    split_req->set_leader(node_id_);
    // set epoch
    split_req->mutable_epoch()->set_version(epoch->version() + 1);

    // no need set con_ver;con_ver is member change
    // split_req->mutable_epoch()->set_version(epoch->conf_ver());

    // set range id
    range->set_id(resp.new_range_id());
    // set range start_key
    range->set_start_key(split_key);
    // range end_key doesn't need to change.

    // set range_epoch
    epoch->set_conf_ver(1);
    epoch->set_version(1);

    auto p0 = range->mutable_peers(0);
    // set range peers
    for (int i = 0; i < psize; i++) {
        auto px = range->mutable_peers(i);
        px->set_id(resp.new_peer_ids(i));

        // Don't consider the role
        if (i > 0 && px->node_id() == node_id_) {
            p0->Swap(px);
        }
    }

    split_req->set_allocated_new_range(range);

    auto ret = Submit(cmd);
    if (!ret.ok()) {
        RANGE_LOG_ERROR("AdminSplit raft submit error: %s", ret.ToString().c_str());
    }
}

Status Range::ApplySplit(const raft_cmdpb::Command &cmd, uint64_t index) {
    RANGE_LOG_INFO("ApplySplit Begin, version: %" PRIu64 ", index: %" PRIu64, meta_.GetVersion(), index);

    const auto& req = cmd.admin_split_req();
    auto ret = meta_.CheckSplit(req.split_key(), cmd.verify_epoch().version());
    if (ret.code() == Status::kStaleEpoch) {
        RANGE_LOG_WARN("ApplySplit(new range: %" PRIu64 ") check failed: %s",
                req.new_range().id(), ret.ToString().c_str());
        return Status::OK();
    } else if (ret.code() == Status::kOutOfBound) {
        // invalid split key, ignore split request
        RANGE_LOG_ERROR("ApplySplit(new range: %" PRIu64 ") check failed: %s",
                       req.new_range().id(), ret.ToString().c_str());
        return Status::OK();
    } else if (!ret.ok()) {
        RANGE_LOG_ERROR("ApplySplit(new range: %" PRIu64 ") check failed: %s",
                       req.new_range().id(), ret.ToString().c_str());
        return ret;
    }

    context_->Statistics()->IncrSplitCount();

    ret = context_->SplitRange(id_, req, index);
    if (!ret.ok()) {
        RANGE_LOG_ERROR("ApplySplit(new range: %" PRIu64 ") create failed: %s",
                        req.new_range().id(), ret.ToString().c_str());
        return ret;
    }

    meta_.Split(req.split_key(), req.epoch().version());
    store_->SetEndKey(req.split_key());

    if (req.leader() == node_id_) {
        ReportSplit(req.new_range());

        // new range report heartbeat
        split_range_id_ = req.new_range().id();
        context_->ScheduleHeartbeat(split_range_id_, false);

        uint64_t rsize = context_->GetSplitPolicy()->SplitSize() >> 1;
        auto rng = context_->FindRange(split_range_id_);
        if (rng != nullptr) {
            rng->SetRealSize(real_size_ - rsize);
        }

        real_size_ = rsize;
    }

    context_->Statistics()->DecrSplitCount();

    RANGE_LOG_INFO("ApplySplit(new range: %" PRIu64 ") End. version:%" PRIu64,
            req.new_range().id(), meta_.GetVersion());

    return ret;
}

Status Range::ForceSplit(uint64_t version, std::string *result_split_key) {
    auto meta = meta_.Get();
    // check version when version ne zero
    if (version != 0 && meta.range_epoch().version() != version) {
        std::ostringstream ss;
        ss << "request version: " << version << ", ";
        ss << "current version: " << meta.range_epoch().version();
        return Status(Status::kStaleEpoch, "force split", ss.str());
    }

    std::string split_key;
    auto mode = context_->GetSplitPolicy()->KeyMode();
    if (mode != SplitKeyMode::kNormal) {
        // TODO: support unnormal mode
        return Status(Status::kNotSupported, "split key mode", SplitKeyModeName(mode));
    } else {
        split_key = FindMiddle(meta.start_key(), meta.end_key());
    }

    if (split_key.empty() || split_key <= meta.start_key() || split_key >= meta.end_key()) {
        std::ostringstream ss;
        ss << "begin key: " << EncodeToHex(meta.start_key()) << ", ";
        ss << "end key: " << EncodeToHex(meta.end_key()) << ", ";
        ss << "split key: " << EncodeToHex(split_key);
        return Status(Status::kUnknown, "could not find split key", ss.str());
    }

    RANGE_LOG_INFO("Force split, start AskSplit. split key: [%s-%s]-%s, version: %" PRIu64,
            EncodeToHex(meta.start_key()).c_str(), EncodeToHex(meta.end_key()).c_str(),
            EncodeToHex(split_key).c_str(), meta.range_epoch().version());

    *result_split_key = split_key;
    AskSplit(std::move(split_key), std::move(meta), true);

    return Status::OK();
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
