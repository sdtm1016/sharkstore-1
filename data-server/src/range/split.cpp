#include <common/ds_config.h>
#include "range.h"

#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "master/worker.h"
#include "server/range_server.h"
#include "server/server.h"

namespace sharkstore {
namespace dataserver {
namespace range {

void Range::CheckSplit(uint64_t size) {
    statis_size_ += size;

    if (!statis_flag_ && statis_size_ > ds_config.range_config.check_size) {
        statis_flag_ = true;
        context_->range_server->StatisPush(meta_.id());
    }
}

void Range::ResetStatisSize() {
    // split size is split size, not half of split size
    // amicable sequence writing and random writing
    // const static uint64_t split_size = ds_config.range_config.split_size >>
    // 1;
    const static uint64_t split_size = ds_config.range_config.split_size;

    std::string split_key;
    metapb::Range *meta = nullptr;

    do {
        sharkstore::shared_lock<sharkstore::shared_mutex> lock(meta_lock_);
        meta = new metapb::Range(meta_);
    } while (false);

    if (ds_config.range_config.access_mode == 0) {
        real_size_ = store_->StatisSize(split_key, split_size);
    } else {
        real_size_ = store_->StatisSize(split_key, split_size, true);
    }

    FLOG_DEBUG("range[%" PRIu64 "] real size: %" PRIu64, meta_.id(),
               real_size_);

    statis_flag_ = false;

    do {
        if (!EpochIsEqual(meta->range_epoch())) {
            FLOG_WARN("range[%" PRIu64 "] ResetStatisSize epoch is changed",
                      meta_.id());
            break;
        }

        statis_size_ = 0;
        // when real size >= max size, we need split with split size
        if (real_size_ >= ds_config.range_config.max_size) {
            return AskSplit(split_key, meta);
        }
    } while (false);

    delete meta;
}

void Range::AskSplit(std::string &key, metapb::Range *meta) {
    assert(!key.empty());
    assert(key >= meta->start_key());
    assert(key < meta->end_key());

    FLOG_DEBUG("ask split %" PRIu64, meta_.id());
    mspb::AskSplitRequest ask;
    ask.set_allocated_range(meta);
    ask.set_split_key(std::move(key));

    context_->master_worker->AsyncAskSplit(ask);
}

void Range::ReportSplit(const metapb::Range &new_range) {
    mspb::ReportSplitRequest report;
    do {
        sharkstore::shared_lock<sharkstore::shared_mutex> lock(meta_lock_);
        report.set_allocated_left(new metapb::Range(meta_));
    } while (false);

    report.set_allocated_right(new metapb::Range(new_range));
    context_->master_worker->AsyncReportSplit(report);
}

void Range::AdminSplit(mspb::AskSplitResponse &resp) {
    if (!EpochIsEqual(resp.range().range_epoch())) {
        FLOG_WARN("range[%" PRIu64 "] AdminSplit epoch is changed", meta_.id());
        return;
    }

    auto &split_key = resp.split_key();

    FLOG_INFO(
        "range[%" PRIu64 "] AdminSplit new_range_id: %" PRIu64 " split_key: %s",
        meta_.id(), resp.new_range_id(), EncodeToHexString(split_key).c_str());

    raft_cmdpb::Command cmd;
    uint64_t seq_id = submit_seq_.fetch_add(1);
    // set cmd_id
    cmd.mutable_cmd_id()->set_node_id(node_id_);
    cmd.mutable_cmd_id()->set_seq(seq_id);
    // set cmd_type
    cmd.set_cmd_type(raft_cmdpb::CmdType::AdminSplit);

    // set cmd admin_split_req
    auto split_req = cmd.mutable_admin_split_req();

    split_req->set_split_key(split_key);

    auto range = resp.release_range();

    auto psize = range->peers_size();
    auto rsize = resp.new_peer_ids_size();
    if (psize != rsize) {
        FLOG_WARN("range[%" PRIu64 "] AdminSplit peers_size no equal",
                  meta_.id());
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
        FLOG_ERROR("range[%" PRIu64 "] AdminSplit raft submit error: %s",
                   meta_.id(), ret.ToString().c_str());
    }
}

Status Range::ApplySplit(const raft_cmdpb::Command &cmd) {
    FLOG_INFO("range[%" PRIu64 "] ApplySplit Begin,"
              " version:%" PRIu64 " conf_ver:%" PRIu64,
              meta_.id(), meta_.range_epoch().version(),
              meta_.range_epoch().conf_ver());

    auto &epoch = cmd.verify_epoch();
    if (epoch.version() < meta_.range_epoch().version()) {
        FLOG_INFO("Range %" PRIu64 " ApplySplit epoch stale,"
                  " verify: %" PRIu64 ", cur: %" PRIu64,
                  meta_.id(), epoch.version(), meta_.range_epoch().version());
        return Status::OK();
    } else if (epoch.version() > meta_.range_epoch().version()) {
        FLOG_ERROR("Range %" PRIu64 " ApplySplit epoch stale,"
                   " verify: %" PRIu64 ", cur: %" PRIu64,
                   meta_.id(), epoch.version(), meta_.range_epoch().version());
        return Status(Status::kStaleEpoch, "epoch is changed", "");
    }

    range_status_->range_split_count++;
    context_->run_status->PushRange(monitor::RangeTag::SplitCount,
                                    range_status_->range_split_count);

    auto &req = cmd.admin_split_req();

    auto ret = context_->range_server->ApplySplit(meta_.id(), req);
    if (ret.ok()) {
        do {
            std::unique_lock<sharkstore::shared_mutex> lock(meta_lock_);
            meta_.set_end_key(req.split_key());
            store_->SetEndKey(req.split_key());

            meta_.mutable_range_epoch()->set_version(req.epoch().version());
        } while (false);

        split_range_id_ = req.new_range().id();

        if (req.leader() == node_id_) {
            ReportSplit(req.new_range());

            // new range report heartbeat
            context_->range_server->LeaderQueuePush(split_range_id_,
                                                    getticks());

            uint64_t rsize = ds_config.range_config.split_size >> 1;
            auto rng = context_->range_server->find(split_range_id_);
            if (rng != nullptr) {
                rng->set_real_size(real_size_ - rsize);
            }

            real_size_ = rsize;

            // specify leader don't trigger function OnLeaderChange
            range_status_->range_leader_count++;

            context_->run_status->PushRange(monitor::RangeTag::LeaderCount,
                                            range_status_->range_leader_count);
        }
    }

    range_status_->range_split_count--;
    context_->run_status->PushRange(monitor::RangeTag::SplitCount,
                                    range_status_->range_split_count);

    FLOG_INFO("range[%" PRIu64 "] ApplySplit End,"
              " version:%" PRIu64 " conf_ver:%" PRIu64,
              meta_.id(), meta_.range_epoch().version(),
              meta_.range_epoch().conf_ver());

    return ret;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
