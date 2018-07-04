#include "range_server.h"

#include <chrono>

#include <common/ds_config.h>
#include <fastcommon/shared_func.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <rocksdb/advanced_options.h>
#include <rocksdb/cache.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/db_ttl.h>
#include <rocksdb/utilities/blob_db/blob_db.h>

#include "base/util.h"
#include "common/ds_config.h"
#include "common/ds_encoding.h"
#include "frame/sf_logger.h"
#include "frame/sf_util.h"
#include "proto/gen/funcpb.pb.h"
#include "proto/gen/metapb.pb.h"
#include "proto/gen/schpb.pb.h"
#include "storage/metric.h"

#include "server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

static const std::string kMetaPathSuffix = "meta";
static const std::string kDataPathSuffix = "data";

int RangeServer::Init(ContextServer *context) {
    FLOG_INFO("RangeServer Init begin ...");

    context_ = context;
    range_status_ = &g_status.range_status;

    // 打开数据db
    if (OpenDB() != 0) {
        FLOG_ERROR("RangeServer Init error ...");
        return -1;
    }

    context_->rocks_db = db_;

    // 打开meta db
    auto meta_path = JoinFilePath({ds_config.rocksdb_config.path, kMetaPathSuffix});
    meta_store_ = new storage::MetaStore(meta_path);
    auto ret = meta_store_->Open();
    if (!ret.ok()) {
        FLOG_ERROR("open meta store failed(%s), path=%s", ret.ToString().c_str(),
                   meta_path.c_str());
        return -1;
    }
    // 保存一下NodeId 到Meta
    assert(context_->node_id != 0);
    ret = meta_store_->SaveNodeID(context_->node_id);
    if (!ret.ok()) {
        FLOG_ERROR("save node id to meta failed(%s)", ret.ToString().c_str());
        return -1;
    } else {
        FLOG_DEBUG("save node_id (%lu) to meta.", context_->node_id);
    }
    context_->meta_store = meta_store_;

    std::vector<std::string> metas;
    ret = meta_store_->GetAllRange(metas);
    if (!ret.ok()) {
        FLOG_ERROR("load local range meta failed(%s)", ret.ToString().c_str());
        return -1;
    }

    if (Recover(metas) != 0) {
        FLOG_ERROR("load local range meta failed");
        return -1;
    }

    FLOG_INFO("RangeServer Init end ...");

    return 0;
}

int RangeServer::Start() {
    FLOG_INFO("RangeServer Start begin ...");

    range_heartbeat_ = std::thread(&RangeServer::Heartbeat, this);

    auto handle = range_heartbeat_.native_handle();
    AnnotateThread(handle, "range_hb");

    range_status_->assigned_worker_threads = ds_config.range_config.worker_threads;

    char name[16];
    for (int i = 0; i < ds_config.range_config.worker_threads; i++) {
        worker_.emplace_back([this] {
            uint64_t range_id = 0;
            while (g_continue_flag) {
                {
                    std::unique_lock<std::mutex> lock(statis_mutex_);
                    if (statis_queue_.empty()) {
                        statis_cond_.wait_for(lock, std::chrono::seconds(5));
                        continue;
                    }

                    range_id = statis_queue_.front();
                    statis_queue_.pop();
                }

                auto range = find(range_id);
                if (range == nullptr) {
                    FLOG_ERROR("RawPut request not found range_id %" PRIu64 " failed",
                               range_id);
                    continue;
                }
                range->ResetStatisSize();
            }

            FLOG_INFO("StatisSize worker thread exit...");
            range_status_->actual_worker_threads--;
        });

        range_status_->actual_worker_threads++;

        auto handle = worker_[i].native_handle();
        sprintf(name, "statis:%d", i);
        AnnotateThread(handle, name);
    }

    FLOG_INFO("RangeServer Start end ...");
    return 0;
}

void RangeServer::Stop() {
    FLOG_INFO("RangeServer Stop begin ...");

    queue_cond_.notify_all();
    statis_cond_.notify_all();

    for (auto &work : worker_) {
        if (work.joinable()) {
            work.join();
        }
    }

    if (range_heartbeat_.joinable()) {
        range_heartbeat_.join();
    }

    CloseDB();

    auto it = ranges_.begin();
    while (it != ranges_.end()) {
        it->second->Shutdown();
        it = ranges_.erase(it);
    }

    if (meta_store_ != nullptr) {
        delete meta_store_;
        meta_store_ = nullptr;
    }

    FLOG_INFO("RangeServer Stop end ...");
}

int RangeServer::OpenDB() {
    print_rocksdb_config();

    // 创建db的父目录
    auto db_path = JoinFilePath({ds_config.rocksdb_config.path, kDataPathSuffix});
    int ret = MakeDirAll(db_path, 0755);
    if (ret != 0) {
        FLOG_ERROR("create rocksdb directory(%s) failed(%s)", db_path.c_str(),
                   strErrno(errno).c_str());
        return -1;
    }

    // rocksdb block cache size
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_size = ds_config.rocksdb_config.block_size;
    context_->block_cache =
        rocksdb::NewLRUCache(ds_config.rocksdb_config.block_cache_size);
    if (ds_config.rocksdb_config.block_cache_size > 0) {
        table_options.block_cache = context_->block_cache;
    }

    if (ds_config.rocksdb_config.cache_index_and_filter_blocks){
        table_options.cache_index_and_filter_blocks = true;
    }

    rocksdb::Options ops;
    ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    if (ds_config.rocksdb_config.row_cache_size > 0){
        context_->row_cache =
                rocksdb::NewLRUCache(ds_config.rocksdb_config.row_cache_size);
        ops.row_cache = context_->row_cache;
    }
    ops.max_open_files = ds_config.rocksdb_config.max_open_files;
    ops.create_if_missing = true;
    ops.use_fsync = true;
    ops.use_adaptive_mutex = true;
    ops.bytes_per_sync = ds_config.rocksdb_config.bytes_per_sync;
    ops.write_buffer_size = ds_config.rocksdb_config.write_buffer_size;
    ops.max_write_buffer_number = ds_config.rocksdb_config.max_write_buffer_number;
    ops.min_write_buffer_number_to_merge =
        ds_config.rocksdb_config.min_write_buffer_number_to_merge;
    ops.max_bytes_for_level_base = ds_config.rocksdb_config.max_bytes_for_level_base;
    ops.max_bytes_for_level_multiplier =
        ds_config.rocksdb_config.max_bytes_for_level_multiplier;
    ops.target_file_size_base = ds_config.rocksdb_config.target_file_size_base;
    ops.target_file_size_multiplier =
        ds_config.rocksdb_config.target_file_size_multiplier;
    ops.max_background_flushes = ds_config.rocksdb_config.max_background_flushes;
    ops.max_background_compactions = ds_config.rocksdb_config.max_background_compactions;
    ops.level0_file_num_compaction_trigger =
        ds_config.rocksdb_config.level0_file_num_compaction_trigger;
    ops.level0_slowdown_writes_trigger =
        ds_config.rocksdb_config.level0_slowdown_writes_trigger;
    ops.level0_stop_writes_trigger = ds_config.rocksdb_config.level0_stop_writes_trigger;
    if (ds_config.rocksdb_config.storage_type == 0){
        if (ds_config.rocksdb_config.ttl == 0) {
            auto ret = rocksdb::DB::Open(ops, db_path, &db_);
            if (!ret.ok()) {
                FLOG_ERROR("open rocksdb(%s) failed(%s)", db_path.c_str(),
                           ret.ToString().c_str());
                return -1;
            }
        } else if (ds_config.rocksdb_config.ttl > 0) {
            FLOG_WARN("rocksdb ttl enabled. ttl=%d", ds_config.rocksdb_config.ttl);
            rocksdb::DBWithTTL *ttl_db = nullptr;
            auto ret =
                rocksdb::DBWithTTL::Open(ops, db_path, &ttl_db, ds_config.rocksdb_config.ttl);
            if (!ret.ok()) {
                FLOG_ERROR("open rocksdb(%s) failed(%s)", db_path.c_str(),
                           ret.ToString().c_str());
                return -1;
            } else {
                db_ = ttl_db;
            }
        } else {
            FLOG_ERROR("invalid rocksdb ttl(%d)", ds_config.rocksdb_config.ttl);
            return -1;
        }
    }else if (ds_config.rocksdb_config.storage_type == 1){
        rocksdb::blob_db::BlobDBOptions blobDBOptions = rocksdb::blob_db::BlobDBOptions();
        blobDBOptions.min_blob_size = ds_config.rocksdb_config.min_blob_size;
        blobDBOptions.enable_garbage_collection = ds_config.rocksdb_config.enable_garbage_collection;
        rocksdb::blob_db::BlobDB *blobDB = nullptr;

        auto ret = rocksdb::blob_db::BlobDB::Open(ops,blobDBOptions,db_path,&blobDB);

        if (!ret.ok()){
            FLOG_ERROR("open rocksdb_blob(%s) failed(%s)", db_path.c_str(),
                       ret.ToString().c_str());
            return -1;
        } else {
            db_ = blobDB;
        }
    }else{
        FLOG_ERROR("invalid rocksdb storage_type(%d)", ds_config.rocksdb_config.storage_type);
        return -1;
    }
    return 0;
}

void RangeServer::CloseDB() {
    if (db_ != nullptr) {
        delete db_;
    }
}

void RangeServer::Clear() {
    FLOG_WARN("clear range data!");

    RemoveDirAll(ds_config.raft_config.log_path);
    RemoveDirAll(ds_config.rocksdb_config.path);
}

void RangeServer::DealTask(common::ProtoMessage *msg) {
    ds_header_t &header = msg->header;

    FLOG_DEBUG(
        "server start deal %s task, sid=%ld, msgid=%ld",
        funcpb::FunctionID_Name(static_cast<funcpb::FunctionID>(header.func_id)).c_str(),
        msg->session_id, msg->header.msg_id);

    switch (header.func_id) {
        case funcpb::kFuncRawGet:
            RawGet(msg);
            break;
        case funcpb::kFuncRawPut:
            RawPut(msg);
            break;
        case funcpb::kFuncRawDelete:
            RawDelete(msg);
            break;
        case funcpb::kFuncInsert:
            Insert(msg);
            break;
        case funcpb::kFuncSelect:
            Select(msg);
            break;
        case funcpb::kFuncDelete:
            Delete(msg);
            break;
        case funcpb::kFuncCreateRange:
            CreateRange(msg);
            break;
        case funcpb::kFuncDeleteRange:
            DeleteRange(msg);
            break;
        case funcpb::kFuncRangeTransferLeader:
            TransferLeader(msg);
            break;
        case funcpb::kFuncUpdateRange:
            UpdateRange(msg);
            break;
        case funcpb::kFuncReplaceRange:
            ReplaceRange(msg);
            break;
        case funcpb::kFuncOfflineRange:
            OfflineRange(msg);
            break;
        case funcpb::kFuncGetPeerInfo:
            GetPeerInfo(msg);
            break;
        case funcpb::kFuncSetNodeLogLevel:
            SetLogLevel(msg);
            break;

        // lock
        case funcpb::kFuncLock:
            Lock(msg);
            break;
        case funcpb::kFuncLockUpdate:
            LockUpdate(msg);
            break;
        case funcpb::kFuncUnlock:
            Unlock(msg);
            break;
        case funcpb::kFuncUnlockForce:
            UnlockForce(msg);
            break;
        case funcpb::kFuncLockScan:
            LockScan(msg);
            break;

        // following for redis commands
        case funcpb::kFuncKvSet:
            KVSet(msg);
            break;
        case funcpb::kFuncKvGet:
            KVGet(msg);
            break;
        case funcpb::kFuncKvBatchSet:
            KVBatchSet(msg);
            break;
        case funcpb::kFuncKvBatchGet:
            KVBatchGet(msg);
            break;
        case funcpb::kFuncKvDel:
            KVDelete(msg);
            break;
        case funcpb::kFuncKvBatchDel:
            KVBatchDelete(msg);
            break;
        case funcpb::kFuncKvRangeDel:
            KVRangeDelete(msg);
            break;
        case funcpb::kFuncKvScan:
            KVScan(msg);
            break;
        default:
            FLOG_ERROR("func id is Invalid %d", header.func_id);
            return context_->socket_session->Send(msg, nullptr);
    }
}

void RangeServer::CreateRange(common::ProtoMessage *msg) {
    schpb::CreateRangeRequest req;
    if (!context_->socket_session->GetMessage(msg->body.data(), msg->body.size(), &req)) {
        FLOG_ERROR("deserialize create range request failed");
        return context_->socket_session->Send(msg, nullptr);
    }

    FLOG_INFO("range[%lu] recv create range from master.", req.range().id());

    errorpb::Error *err = nullptr;
    auto resp = new schpb::CreateRangeResponse;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

        auto it = ranges_.find(req.range().id());
        if (it != ranges_.end()) {
            FLOG_WARN("range[%" PRIu64 "] already exist.", req.range().id());

            if (!it->second->EpochIsEqual(req.range())) {
                err = new errorpb::Error;
                auto meta = new metapb::Range(it->second->options());

                err->mutable_stale_range()->set_allocated_range(meta);
                err->set_message("range already exist but epoch no equal");
            }

            break;
        }

        std::string value;
        if (!req.range().SerializeToString(&value)) {
            err = new errorpb::Error;
            err->set_message("create range seriaize meta failed");

            FLOG_ERROR("create range seriaize meta failed");
            break;
        }

        auto ret = meta_store_->AddRange(req.range().id(), value);
        if (!ret.ok()) {
            err = new errorpb::Error;
            err->set_message("create range seriaize meta failed");

            FLOG_ERROR("create range save meta failed: %s", ret.ToString().c_str());
            break;
        }

        ret = CreateRange(req.range());
        if (!ret.ok() && ret.code() != Status::kDuplicate) {
            err = new errorpb::Error;
            err->set_message(ret.ToString());

            meta_store_->DelRange(req.range().id());
            FLOG_ERROR("create range failed %" PRIu64, req.range().id());
            break;
        }

    } while (false);

    if (err != nullptr) {
        resp->mutable_header()->set_allocated_error(err);
    }

    return context_->socket_session->Send(msg, resp);
}

Status RangeServer::CreateRange(const metapb::Range &range, uint64_t leader) {
    FLOG_DEBUG("new range: id=%" PRIu64 ", start=%s, end=%s,"
               " version=%" PRIu64 ", conf_ver=%" PRIu64,
               range.id(), EncodeToHexString(range.start_key()).c_str(),
               EncodeToHexString(range.end_key()).c_str(), range.range_epoch().version(),
               range.range_epoch().conf_ver());

    if (range.peers_size() == 0) {
        FLOG_ERROR("CreateRange range[%" PRIu64 "] failed. peers is zero", range.id());
        return Status(Status::kInvalidArgument, "invalid peer size", "0");
    }

    auto it = ranges_.find(range.id());
    if (it != ranges_.end()) {
        FLOG_WARN("CreateRange range[%" PRIu64 "] is exist.", range.id());
        return Status(Status::kDuplicate, "range is exist", "");
    }

    auto rng = std::make_shared<range::Range>(context_, range);
    if (rng == nullptr) {
        FLOG_ERROR("create new range[%" PRIu64 "] failed.", range.id());
        return Status(Status::kNoMem, "new range null", "");
    }
    // 初始化range
    auto ret = rng->Initialize(range_status_, leader);
    if (!ret.ok()) {
        FLOG_ERROR("initialize range[%" PRIu64 "] failed: %s", range.id(),
                   ret.ToString().c_str());
        return ret;
    }

    ranges_[range.id()] = rng;

    FLOG_INFO("create new range[%" PRIu64 "] success.", range.id());

    range_status_->range_count++;
    context_->run_status->PushRange(monitor::RangeTag::RangeCount,
                                    range_status_->range_count);

    return ret;
}

void RangeServer::DeleteRange(common::ProtoMessage *msg) {
    schpb::DeleteRangeRequest req;
    if (!context_->socket_session->GetMessage(msg->body.data(), msg->body.size(), &req)) {
        FLOG_ERROR("deserialize delete range request failed");
        return context_->socket_session->Send(msg, nullptr);
    }

    auto resp = new schpb::DeleteRangeResponse;
    if (DeleteRange(req.range_id()) != 0) {
        auto err = resp->mutable_header()->mutable_error();
        err->set_message("delete range failed");
    }

    return context_->socket_session->Send(msg, resp);
}

int RangeServer::DeleteRange(uint64_t range_id) {
    std::shared_ptr<range::Range> rng;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

        meta_store_->DelRange(range_id);

        auto it = ranges_.find(range_id);
        if (it == ranges_.end()) {
            FLOG_WARN("delete range[%" PRIu64 "] not found.", range_id);
            return 0;
        }

        rng = it->second;

        rng->Shutdown();
        auto s = rng->Truncate();
        if (!s.ok()) {
            FLOG_INFO("delete range[%" PRIu64 "] truncate failed.", range_id);
            return -1;
        }

        ranges_.erase(it);

    } while (false);

    FLOG_INFO("delete range[%" PRIu64 "] success.", range_id);

    range_status_->range_count--;
    context_->run_status->PushRange(monitor::RangeTag::RangeCount,
                                    range_status_->range_count);
    return 0;
}

void RangeServer::OfflineRange(common::ProtoMessage *msg) {
    schpb::OfflineRangeRequest req;
    if (!context_->socket_session->GetMessage(msg->body.data(), msg->body.size(), &req)) {
        FLOG_ERROR("deserialize offline range request failed");
        return context_->socket_session->Send(msg, nullptr);
    }

    auto resp = new schpb::OfflineRangeResponse;
    if (OfflineRange(req.rangeid()) != 0) {
        auto err = resp->mutable_header()->mutable_error();
        err->set_message("offline range failed");
    }

    context_->socket_session->Send(msg, resp);
}

int RangeServer::OfflineRange(uint64_t range_id) {
    FLOG_INFO("offline range[%" PRIu64 "] success.", range_id);

    std::shared_ptr<range::Range> rng;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

        auto it = ranges_.find(range_id);
        if (it == ranges_.end()) {
            FLOG_WARN("offline range[%" PRIu64 "] not found.", range_id);
            return -1;
        }

        rng = it->second;

        // TODO::close range
        // current: Shutdown replaced
        rng->Shutdown();
        ranges_.erase(it);

    } while (false);

    range_status_->range_count--;
    context_->run_status->PushRange(monitor::RangeTag::RangeCount,
                                    range_status_->range_count);
    return 0;
}

int RangeServer::CloseRange(uint64_t range_id) {
    std::shared_ptr<range::Range> rng;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

        meta_store_->DelRange(range_id);

        auto it = ranges_.find(range_id);
        if (it == ranges_.end()) {
            FLOG_WARN("close range[%" PRIu64 "] not found.", range_id);
            return 0;
        }

        rng = it->second;

        // TODO::close range
        // current: Shutdown replaced
        rng->Shutdown();
        ranges_.erase(it);

    } while (false);

    FLOG_INFO("close range[%" PRIu64 "] success.", range_id);

    range_status_->range_count--;
    context_->run_status->PushRange(monitor::RangeTag::RangeCount,
                                    range_status_->range_count);
    return 0;
}

void RangeServer::UpdateRange(common::ProtoMessage *msg) {
    schpb::UpdateRangeRequest req;
    if (!context_->socket_session->GetMessage(msg->body.data(), msg->body.size(), &req)) {
        FLOG_ERROR("deserialize update range request failed");
        return context_->socket_session->Send(msg, nullptr);
    }

    auto resp = new schpb::UpdateRangeResponse;
    do {
        if (CloseRange(req.range().id()) != 0) {
            auto err = resp->mutable_header()->mutable_error();
            err->set_message("update range failed");
            break;
        }
        auto ret = CreateRange(req.range());
        if (!ret.ok()) {
            auto err = resp->mutable_header()->mutable_error();
            err->set_message("update range failed");
        }
    } while (false);

    context_->socket_session->Send(msg, resp);
}

void RangeServer::ReplaceRange(common::ProtoMessage *msg) {
    schpb::ReplaceRangeRequest req;
    if (!context_->socket_session->GetMessage(msg->body.data(), msg->body.size(), &req)) {
        FLOG_ERROR("deserialize replace range request failed");
        return context_->socket_session->Send(msg, nullptr);
    }

    FLOG_WARN("start update range. old=%lu, new=%lu.", req.old_range_id(),
              req.new_range().id());

    auto resp = new schpb::ReplaceRangeResponse;
    do {
        if (CloseRange(req.old_range_id()) != 0) {
            auto err = resp->mutable_header()->mutable_error();
            err->set_message("close old range failed");
            break;
        }
        auto ret = CreateRange(req.new_range());
        if (!ret.ok()) {
            auto err = resp->mutable_header()->mutable_error();
            err->set_message("create range failed");
        }
    } while (false);

    context_->socket_session->Send(msg, resp);
}

void RangeServer::TransferLeader(common::ProtoMessage *msg) {
    schpb::TransferRangeLeaderRequest req;
    if (!context_->socket_session->GetMessage(msg->body.data(), msg->body.size(), &req)) {
        FLOG_ERROR("deserialize transfer leader request failed");
        return context_->socket_session->Send(msg, nullptr);
    }

    auto resp = new schpb::TransferRangeLeaderResponse;

    auto range = find(req.range_id());
    if (range == nullptr) {
        FLOG_ERROR("TransferLeade request not found range_id %" PRIu64 " failed",
                   req.range_id());
    } else {
        range->TransferLeader();
    }

    context_->socket_session->Send(msg, resp);
}

void RangeServer::GetPeerInfo(common::ProtoMessage *msg) {
    schpb::GetPeerInfoRequest req;

    if (!context_->socket_session->GetMessage(msg->body.data(), msg->body.size(), &req)) {
        FLOG_ERROR("deserialize transfer leader request failed");
        return context_->socket_session->Send(msg, nullptr);
    }

    auto resp = new schpb::GetPeerInfoResponse;

    raft::RaftStatus peer_info;

    auto range = find(req.range_id());
    if (range == nullptr) {
        FLOG_ERROR("TransferLeade request not found range_id %" PRIu64 " failed",
                   req.range_id());

        auto err = resp->mutable_header()->mutable_error();
        err->set_message("range not found");
        err->mutable_range_not_found()->set_range_id(req.range_id());
    } else {
        range->GetPeerInfo(&peer_info);
        resp->set_index(peer_info.index);
        resp->set_term(peer_info.term);
        resp->set_commit(peer_info.commit);

        auto replica = resp->mutable_replica();
        range->GetReplica(replica);
    }

    context_->socket_session->Send(msg, resp);
}

void RangeServer::SetLogLevel(common::ProtoMessage *msg) {
    schpb::SetNodeLogLevelRequest req;

    if (!context_->socket_session->GetMessage(msg->body.data(), msg->body.size(), &req)) {
        FLOG_ERROR("deserialize transfer leader request failed");
        return context_->socket_session->Send(msg, nullptr);
    }

    auto resp = new schpb::SetNodeLogLevelResponse;
    char level[8];
    snprintf(level, 8, "%s", req.level().c_str());
    set_log_level(level);

    context_->socket_session->Send(msg, resp);
}

std::shared_ptr<range::Range> RangeServer::find(uint64_t range_id) {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);

    auto it = ranges_.find(range_id);
    if (it == ranges_.end()) {
        for (auto itr = ranges_.begin(); itr != ranges_.end(); itr++) {
            FLOG_DEBUG("current range cache range_id:%" PRIu64 " ", itr->first);
        }
        return nullptr;
    }

    if (!it->second->valid()) {
        FLOG_WARN("RangeHeartbeat range_id %" PRIu64 " is invalid", range_id);

        return nullptr;
    }

    return it->second;
}

void RangeServer::RawGet(common::ProtoMessage *msg) {
    kvrpcpb::DsKvRawGetRequest req;
    kvrpcpb::DsKvRawGetResponse *resp;

    auto range = CheckAndDecodeRequest("RawGet", req, resp, msg);
    if (range != nullptr) {
        range->RawGet(msg, req);
    }
}

void RangeServer::RawPut(common::ProtoMessage *msg) {
    kvrpcpb::DsKvRawPutRequest req;
    kvrpcpb::DsKvRawPutResponse *resp;

    auto range = CheckAndDecodeRequest("RawPut", req, resp, msg);
    if (range != nullptr) {
        range->RawPut(msg, req);
    }
}

void RangeServer::RawDelete(common::ProtoMessage *msg) {
    kvrpcpb::DsKvRawDeleteRequest req;
    kvrpcpb::DsKvRawDeleteResponse *resp;

    auto range = CheckAndDecodeRequest("RawDelete", req, resp, msg);
    if (range != nullptr) {
        range->RawDelete(msg, req);
    }
}

void RangeServer::Insert(common::ProtoMessage *msg) {
    kvrpcpb::DsInsertRequest req;
    kvrpcpb::DsInsertResponse *resp;

    auto range = CheckAndDecodeRequest("Insert", req, resp, msg);
    if (range != nullptr) {
        range->Insert(msg, req);
    }
}

void RangeServer::Select(common::ProtoMessage *msg) {
    kvrpcpb::DsSelectRequest req;
    kvrpcpb::DsSelectResponse *resp;

    auto range = CheckAndDecodeRequest("Select", req, resp, msg);
    if (range != nullptr) {
        range->Select(msg, req);
    }
}

void RangeServer::Delete(common::ProtoMessage *msg) {
    kvrpcpb::DsDeleteRequest req;
    kvrpcpb::DsKvDeleteResponse *resp;

    auto range = CheckAndDecodeRequest("Delete", req, resp, msg);
    if (range != nullptr) {
        range->Delete(msg, req);
    }
}

template <class RequestT, class ResponseT>
std::shared_ptr<range::Range> RangeServer::CheckAndDecodeRequest(
    const char *func_name, RequestT &request, ResponseT *&respone,
    common::ProtoMessage *msg) {
    if (!context_->socket_session->GetMessage(msg->body.data(), msg->body.size(),
                                              &request)) {
        FLOG_ERROR("deserialize %s request failed", func_name);
        context_->socket_session->Send(msg, nullptr);
        return nullptr;
    }

    FLOG_DEBUG("%s called. req: %s", func_name, request.DebugString().c_str());

    // check timeout
    if (msg->expire_time < getticks()) {
        FLOG_WARN("%s request timeout", func_name);
        respone = new ResponseT;
        TimeOut(request.header(), respone->mutable_header());
        context_->socket_session->Send(msg, respone);
        return nullptr;
    }

    auto range = find(request.header().range_id());
    if (range == nullptr) {
        FLOG_ERROR("%s request not found range_id %" PRIu64 " failed", func_name,
                   request.header().range_id());
        respone = new ResponseT;
        RangeNotFound(request.header(), respone->mutable_header());
        context_->socket_session->Send(msg, respone);
        return nullptr;
    }

    return range;
}

void RangeServer::Lock(common::ProtoMessage *msg) {
    kvrpcpb::DsLockRequest req;
    kvrpcpb::DsLockResponse *resp;

    auto range = CheckAndDecodeRequest("Lock", req, resp, msg);
    if (range != nullptr) {
        range->Lock(msg, req);
    }
}

void RangeServer::LockUpdate(common::ProtoMessage *msg) {
    kvrpcpb::DsLockUpdateRequest req;
    kvrpcpb::DsLockUpdateResponse *resp;

    auto range = CheckAndDecodeRequest("LockUpdate", req, resp, msg);
    if (range != nullptr) {
        range->LockUpdate(msg, req);
    }
}

void RangeServer::Unlock(common::ProtoMessage *msg) {
    kvrpcpb::DsUnlockRequest req;
    kvrpcpb::DsUnlockResponse *resp;

    auto range = CheckAndDecodeRequest("Unlock", req, resp, msg);
    if (range != nullptr) {
        range->Unlock(msg, req);
    }
}

void RangeServer::UnlockForce(common::ProtoMessage *msg) {
    kvrpcpb::DsUnlockForceRequest req;
    kvrpcpb::DsUnlockForceResponse *resp;

    auto range = CheckAndDecodeRequest("UnlockForce", req, resp, msg);
    if (range != nullptr) {
        range->UnlockForce(msg, req);
    }
}

void RangeServer::LockScan(common::ProtoMessage *msg) {
    kvrpcpb::DsLockScanRequest req;
    kvrpcpb::DsLockScanResponse *resp;

    auto range = CheckAndDecodeRequest("LockScan", req, resp, msg);
    if (range != nullptr) {
        range->LockScan(msg, req);
  }
}

void RangeServer::KVSet(common::ProtoMessage *msg) {
    kvrpcpb::DsKvSetRequest req;
    kvrpcpb::DsKvSetResponse *resp;

    auto range = CheckAndDecodeRequest("KVSet", req, resp, msg);
    if (range != nullptr) {
        range->KVSet(msg, req);
    }
}

void RangeServer::KVGet(common::ProtoMessage *msg) {
    kvrpcpb::DsKvGetRequest req;
    kvrpcpb::DsKvGetResponse *resp;

    auto range = CheckAndDecodeRequest("KVGet", req, resp, msg);
    if (range != nullptr) {
        range->KVGet(msg, req);
    }
}

void RangeServer::KVBatchSet(common::ProtoMessage *msg) {
    kvrpcpb::DsKvBatchSetRequest req;
    kvrpcpb::DsKvBatchSetResponse *resp;

    auto range = CheckAndDecodeRequest("KVBatchSet", req, resp, msg);
    if (range != nullptr) {
        range->KVBatchSet(msg, req);
    }
}

void RangeServer::KVBatchGet(common::ProtoMessage *msg) {
    kvrpcpb::DsKvBatchGetRequest req;
    kvrpcpb::DsKvBatchGetResponse *resp;

    auto range = CheckAndDecodeRequest("KVBatchGet", req, resp, msg);
    if (range != nullptr) {
        range->KVBatchGet(msg, req);
    }
}

void RangeServer::KVDelete(common::ProtoMessage *msg) {
    kvrpcpb::DsKvDeleteRequest req;
    kvrpcpb::DsKvDeleteResponse *resp;

    auto range = CheckAndDecodeRequest("KVDelete", req, resp, msg);
    if (range != nullptr) {
        range->KVDelete(msg, req);
    }
}

void RangeServer::KVBatchDelete(common::ProtoMessage *msg) {
    kvrpcpb::DsKvBatchDeleteRequest req;
    kvrpcpb::DsKvBatchDeleteResponse *resp;

    auto range = CheckAndDecodeRequest("KVBatchDelete", req, resp, msg);
    if (range != nullptr) {
        range->KVBatchDelete(msg, req);
    }
}

void RangeServer::KVRangeDelete(common::ProtoMessage *msg) {
    kvrpcpb::DsKvRangeDeleteRequest req;
    kvrpcpb::DsKvRangeDeleteResponse *resp;

    auto range = CheckAndDecodeRequest("KVRangeDelete", req, resp, msg);
    if (range != nullptr) {
        range->KVRangeDelete(msg, req);
    }
}

void RangeServer::KVScan(common::ProtoMessage *msg) {
    kvrpcpb::DsKvScanRequest req;
    kvrpcpb::DsKvScanResponse *resp;

    auto range = CheckAndDecodeRequest("KVScan", req, resp, msg);
    if (range != nullptr) {
        range->KVScan(msg, req);
    }
}

Status RangeServer::ApplySplit(uint64_t old_range_id,
                               const raft_cmdpb::SplitRequest &req) {
    auto rng = find(old_range_id);
    if (rng == nullptr) {
        FLOG_ERROR("ApplySplit not found range_id %" PRIu64 " failed", old_range_id);
        return Status(Status::kNotFound, "range not found", "");
    }

    bool is_exist = false;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);
        auto ret = CreateRange(req.new_range(), req.leader());
        if (ret.code() == Status::kDuplicate) {
            is_exist = true;
            break;
        }

        if (!ret.ok()) {
            FLOG_ERROR("ApplySplit old_range_id: %" PRIu64 " new_range_id: %" PRIu64
                       " failed",
                       old_range_id, req.new_range().id());
            return ret;
        }
    } while (false);

    std::map<uint64_t, std::string> batch_range;

    metapb::Range meta = rng->options();
    meta.set_end_key(req.split_key());

    meta.mutable_range_epoch()->set_version(req.epoch().version());

    std::string value;
    if (meta.SerializeToString(&value)) {
        batch_range.emplace(old_range_id, std::move(value));
    } else {
        FLOG_ERROR("ApplySplit range_id %" PRIu64 " failed", old_range_id);
        DeleteRange(req.new_range().id());
        return Status(Status::kInvalidArgument, "seriaize meta error", "");
    }

    if (!is_exist) {
        if (req.new_range().SerializeToString(&value)) {
            batch_range.emplace(req.new_range().id(), std::move(value));
        } else {
            FLOG_ERROR("ApplySplit range_id %" PRIu64 " failed", old_range_id);
            DeleteRange(req.new_range().id());
            return Status(Status::kInvalidArgument, "seriaize meta error", "");
        }
    }

    auto ret = meta_store_->BatchAddRange(batch_range);
    if (!ret.ok()) {
        FLOG_ERROR("ApplySplit batch add range failed");
        if (!is_exist) {
            DeleteRange(req.new_range().id());
        }
        return ret;
    }

    return ret;
}

void RangeServer::TimeOut(const kvrpcpb::RequestHeader &req,
                          kvrpcpb::ResponseHeader *resp) {
    errorpb::Error *err = new errorpb::Error;

    err->set_message("time out");
    err->mutable_timeout();

    context_->socket_session->SetResponseHeader(req, resp, err);
}

void RangeServer::RangeNotFound(const kvrpcpb::RequestHeader &req,
                                kvrpcpb::ResponseHeader *resp) {
    errorpb::Error *err = new errorpb::Error;

    err->set_message("range not found");
    err->mutable_range_not_found()->set_range_id(req.range_id());

    context_->socket_session->SetResponseHeader(req, resp, err);
}

int RangeServer::Recover(std::vector<std::string> &metas) {
    FLOG_DEBUG("Recover meta range size %lu", metas.size());
    for (auto &m : metas) {
        metapb::Range meta;
        if (!context_->socket_session->GetMessage(m.data(), m.size(), &meta)) {
            FLOG_ERROR("Recover deserialize failed");
            return -1;
        }
        FLOG_DEBUG("Recover meta range id=%" PRIu64, meta.id());
        if (!CreateRange(meta).ok()) {
            FLOG_ERROR("Recover CreateRange failed,id=%" PRIu64, meta.id());
            if (ds_config.range_config.recover_skip_fail > 0) {
                continue;
            } else {
                return -1;
            }
        }
    }

    return 0;
}

void RangeServer::Heartbeat() {
    uint64_t range_id = 0;
    int interval = ds_config.hb_config.range_interval;

    while (g_continue_flag) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (range_heartbeat_queue_.empty()) {
                queue_cond_.wait_for(lock, std::chrono::seconds(interval));
                continue;
            }

            auto hb = range_heartbeat_queue_.top();

            time_t now = getticks();
            if (hb.first > now) {
                auto intval = std::chrono::milliseconds(hb.first - now);
                queue_cond_.wait_for(lock, intval);
                continue;
            }

            range_heartbeat_queue_.pop();
            range_id = hb.second;
        }

        auto range = find(range_id);
        if (range != nullptr) {
            range->Heartbeat();
        }
    }

    FLOG_INFO("RangeHeartBeat thread exit...");
}

void RangeServer::LeaderQueuePush(uint64_t leader, time_t expire) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    range_heartbeat_queue_.emplace(expire, leader);

    queue_cond_.notify_all();
}

void RangeServer::StatisPush(uint64_t range_id) {
    std::lock_guard<std::mutex> lock(statis_mutex_);
    statis_queue_.push(range_id);
    statis_cond_.notify_all();
}

metapb::Range *RangeServer::GetRangeMeta(uint64_t range_id) {
    auto rng = find(range_id);
    if (rng != nullptr) {
        return new metapb::Range(rng->options());
    }

    return nullptr;
}

void RangeServer::OnNodeHeartbeatResp(const mspb::NodeHeartbeatResponse &resp) {
    // TODO: clear replicas
    FLOG_INFO("Recv NodeHeartbeat Response from master server.");
}

void RangeServer::OnRangeHeartbeatResp(const mspb::RangeHeartbeatResponse &resp) {
    if (!resp.has_task()) {
        return;
    }

    auto range = find(resp.range_id());
    if (range == nullptr) {
        FLOG_ERROR("RangeHeartbeat Task not found range_id %" PRIu64 " failed",
                   resp.range_id());
        return;
    }

    if (!range->valid()) {
        FLOG_WARN("RangeHeartbeat range_id %" PRIu64 " is invalid", resp.range_id());
        return;
    }

    switch (resp.task().type()) {
        case taskpb::TaskType::EmptyTask:
            FLOG_DEBUG("RangeHeartbeat task empty.");
            break;
        case taskpb::TaskType::RangeMerge:
            FLOG_DEBUG("RangeHeartbeat task RangeMerge.");
            // TODO
            break;
        case taskpb::TaskType::RangeDelete:
            FLOG_DEBUG("RangeHeartbeat task RangeDelete. range id: %" PRIu64,
                       resp.range_id());
            DeleteRange(resp.range_id());
            break;
        case taskpb::TaskType::RangeLeaderTransfer:
            FLOG_DEBUG("RangeHeartbeat task RangeLeaderTransfer. range id: %" PRIu64,
                       resp.range_id());
            // TODO
            // master undefinded
            break;
        case taskpb::TaskType::RangeAddPeer:
            FLOG_DEBUG("RangeHeartbeat task RangeAddPeer. range id: %" PRIu64,
                       resp.range_id());
            range->AddPeer(resp.task().range_add_peer().peer());
            break;
        case taskpb::TaskType::RangeDelPeer:
            FLOG_DEBUG("RangeHeartbeat task RangeDelPeer. range id: %" PRIu64,
                       resp.range_id());
            range->DelPeer(resp.task().range_del_peer().peer());
            break;
        default:
            FLOG_ERROR("RangeHeartbeat task error. range id: %" PRIu64 " task type: %d",
                       resp.range_id(), resp.task().type());
    }
}

void RangeServer::OnAskSplitResp(const mspb::AskSplitResponse &resp) {
    if (resp.has_header() && resp.header().has_error()) {
        FLOG_ERROR("AskSplit response has error range_id %" PRIu64, resp.range().id());
        return;
    }

    FLOG_INFO("range[%lu] recv AskSplit response from master.", resp.range().id());

    auto range = find(resp.range().id());
    if (range == nullptr) {
        FLOG_ERROR("AdminSplit not found range_id %" PRIu64 " failed", resp.range().id());
        return;
    }

    // TODO: remove this copy
    auto copy_resp = resp;
    range->AdminSplit(copy_resp);
}

void RangeServer::CollectNodeHeartbeat(mspb::NodeHeartbeatRequest *req) {
    context_->run_status->SetHardDiskInfo();

    req->set_node_id(context_->node_id);

    auto stats = req->mutable_stats();
    stats->set_range_count(range_status_->range_count);
    stats->set_range_leader_count(range_status_->range_leader_count);
    stats->set_range_split_count(range_status_->range_split_count);

    raft::ServerStatus rss;
    context_->raft_server->GetStatus(&rss);
    stats->set_sending_snap_count(rss.total_snap_sending);
    stats->set_receiving_snap_count(rss.total_snap_applying);
    stats->set_applying_snap_count(rss.total_snap_applying);

    stats->set_capacity(g_status.hard_info.total_size);
    stats->set_used_size(g_status.hard_info.used_size);
    stats->set_available(g_status.hard_info.free_size);

    storage::MetricStat mstat;
    storage::Metric::CollectAll(&mstat);
    stats->set_keys_read(mstat.keys_read_per_sec);
    stats->set_bytes_read(mstat.bytes_read_per_sec);
    stats->set_keys_written(mstat.keys_write_per_sec);
    stats->set_bytes_written(mstat.bytes_write_per_sec);

    stats->set_is_busy(false);
}

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
