#include "range.h"
#include "server/range_server.h"

namespace sharkstore {
namespace dataserver {
namespace range {

kvrpcpb::LockValue *Range::LockGet(const std::string &key) {
    FLOG_DEBUG("lock get: key[%s]", EncodeToHexString(key).c_str());
    std::string val;
    auto ret = store_->Get(key, &val);
    if (!ret.ok()) {
        FLOG_WARN("lock get: no key[%s]", EncodeToHexString(key).c_str());
        return nullptr;
    }

    FLOG_DEBUG("lock get ok: key[%s] val[%s]", EncodeToHexString(key).c_str(),
               EncodeToHexString(val).c_str());
    auto req = new (kvrpcpb::LockValue);
    if (!req->ParseFromString(val)) {
        FLOG_WARN("key[%s] value parse failed", EncodeToHexString(key).c_str());
        return nullptr;
    }

    if (req->delete_time() > 0 && req->delete_time() <= getticks()) {
        FLOG_WARN("key[%s] deteled at time %ld", EncodeToHexString(key).c_str(),
                  req->delete_time());
        return nullptr;
    }

    if (getticks() - req->update_time() > DEFAULT_LOCK_DELETE_TIME_MILLSEC) {
        FLOG_WARN("key[%s] deteled last update time %ld > 3s",
                  EncodeToHexString(key).c_str(), req->update_time());
        return nullptr;
    }

    FLOG_DEBUG("lock get parse: key[%s] val[%s]",
               EncodeToHexString(key).c_str(), req->DebugString().c_str());
    return req;
}

void Range::Lock(common::ProtoMessage *msg, kvrpcpb::DsLockRequest &req) {
    FLOG_DEBUG("lock request: %s", req.DebugString().c_str());
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    auto &key = req.req().key();
    errorpb::Error *err = nullptr;

    do {
        if (!VerifyLeader(err)) {
            FLOG_WARN("range[%" PRIu64 "] Lock error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        if (!KeyInRange(key, err)) {
            FLOG_WARN("range[%" PRIu64 "] Lock error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        auto epoch = req.header().range_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("range[%" PRIu64 "] Lock error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::Lock);
            cmd.set_allocated_lock_req(req.release_req());
        });

        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] Lock raft submit error: %s",
                       meta_.id(), ret.ToString().c_str());

            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        auto resp = new kvrpcpb::DsLockResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyLock(const raft_cmdpb::Command &cmd) {
    FLOG_DEBUG("apply lock: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;

    auto req = cmd.lock_req();
    auto resp = new (kvrpcpb::DsLockResponse);
    do {
        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyLock error: %s", meta_.id(),
                      err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }
        auto val = LockGet(req.key());
        // 允许相同id的重复执行lock
        if (val != nullptr) {
            if (val->delete_flag()) {
                FLOG_WARN("Range %" PRIu64
                          "  ApplyLock error: lock [%s] is force unlocked",
                          meta_.id(), req.key().c_str());
                resp->mutable_resp()->set_code(LOCK_IS_FORCE_UNLOCKED);
                resp->mutable_resp()->set_error("be force unlocked");
                resp->mutable_resp()->set_value(val->value());
                resp->mutable_resp()->set_update_time(val->update_time());
                break;
            }
            if (req.value().id() != val->id()) {
                FLOG_WARN("Range %" PRIu64
                          "  ApplyLock error: lock [%s] is existed",
                          meta_.id(), req.key().c_str());
                resp->mutable_resp()->set_code(LOCK_EXISTED);
                resp->mutable_resp()->set_error("already locked");
                resp->mutable_resp()->set_value(val->value());
                resp->mutable_resp()->set_update_time(val->update_time());
                break;
            }
        }

        auto btime = get_micro_second();
        req.mutable_value()->set_update_time(getticks());
        if (req.value().delete_time() != 0) {
            req.mutable_value()->set_delete_time(req.value().delete_time() +
                                                 getticks());
        }
        ret = store_->Put(req.key(), req.value().SerializeAsString());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            FLOG_ERROR("ApplyLock failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("lock failed");
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = req.key().size() + req.value().ByteSizeLong();
            CheckSplit(len);
        }
        delete val;

        FLOG_INFO("Range %" PRIu64 "  ApplyLock: lock [%s] is locked by %s",
                  meta_.id(), req.key().c_str(), req.by().c_str());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        SendResponse(resp, cmd, resp->has_resp() ? resp->resp().code() : 0,
                     err);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::LockUpdate(common::ProtoMessage *msg,
                       kvrpcpb::DsLockUpdateRequest &req) {
    FLOG_DEBUG("lock update: %s", req.DebugString().c_str());
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    auto &key = req.req().key();
    errorpb::Error *err = nullptr;

    do {
        if (!VerifyLeader(err)) {
            FLOG_WARN("range[%" PRIu64 "] LockUpdate error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        if (!KeyInRange(key, err)) {
            FLOG_WARN("range[%" PRIu64 "] LockUpdate error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        auto epoch = req.header().range_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("range[%" PRIu64 "] LockUpdate error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::LockUpdate);
            cmd.set_allocated_lock_update_req(req.release_req());
        });

        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] LockUpdate raft submit error: %s",
                       meta_.id(), ret.ToString().c_str());

            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        auto resp = new kvrpcpb::DsLockUpdateResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyLockUpdate(const raft_cmdpb::Command &cmd) {
    FLOG_DEBUG("apply lock update: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;

    auto &req = cmd.lock_update_req();
    auto resp = new (kvrpcpb::DsLockUpdateResponse);
    do {
        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyLockUpdate error: %s",
                      meta_.id(), err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }

        auto val = LockGet(req.key());
        if (val == nullptr) {
            FLOG_WARN("ApplyLockUpdate error: lock [%s] is not existed",
                      req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_NOT_EXIST);
            resp->mutable_resp()->set_error("not exist");
            break;
        }
        if (val->delete_flag()) {
            FLOG_WARN("ApplyLockUpdate error: lock [%s] is force unlocked",
                      req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_IS_FORCE_UNLOCKED);
            resp->mutable_resp()->set_error("be force unlocked");
            break;
        }
        if (req.id() != val->id()) {
            FLOG_WARN("ApplyLockUpdate error: lock [%s] can not update with id "
                      "%s != %s",
                      req.key().c_str(), req.id().c_str(), val->id().c_str());
            resp->mutable_resp()->set_code(LOCK_ID_MISMATCHED);
            resp->mutable_resp()->set_error("wrong id: " + val->id());
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        val->set_update_time(getticks() + req.update_time());
        if (req.update_value().size() != 0) {
            val->set_value(req.update_value());
        }

        auto btime = get_micro_second();
        ret = store_->Put(req.key(), val->SerializeAsString());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            FLOG_ERROR("ApplyLockUpdate failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("lock update failed");
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = req.key().size() + req.ByteSizeLong();
            CheckSplit(len);
        }
        delete val;

        FLOG_INFO("Range %" PRIu64 "  ApplyLockUpdate: lock [%s] is update",
                  meta_.id(), req.key().c_str());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        SendResponse(resp, cmd, resp->has_resp() ? resp->resp().code() : 0,
                     err);

    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::Unlock(common::ProtoMessage *msg, kvrpcpb::DsUnlockRequest &req) {
    FLOG_DEBUG("unlock: %s", req.DebugString().c_str());
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    auto &key = req.req().key();
    errorpb::Error *err = nullptr;
    do {
        if (!VerifyLeader(err)) {
            break;
        }
        if (!KeyInRange(key, err)) {
            break;
        }

        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::Unlock);
            cmd.set_allocated_unlock_req(req.release_req());
        });
        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] Unlock raft submit error: %s",
                       meta_.id(), ret.ToString().c_str());
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] Unlock error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new kvrpcpb::DsUnlockResponse;
        SendError(msg, req.header(), resp, err);
    }
    return;
}

Status Range::ApplyUnlock(const raft_cmdpb::Command &cmd) {
    FLOG_DEBUG("apply unlock: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;

    auto &req = cmd.unlock_req();
    auto resp = new (kvrpcpb::DsUnlockResponse);
    do {
        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyUnlock error: %s", meta_.id(),
                      err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }

        auto val = LockGet(req.key());
        if (val == nullptr) {
            FLOG_WARN("Range %" PRIu64
                      "  ApplyUnlock error: lock [%s] is not existed",
                      meta_.id(), req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_NOT_EXIST);
            resp->mutable_resp()->set_error("not exist");
            break;
        }
        // 允许force unlock的锁被owner正常解锁
        if (val->delete_flag()) {
            if (req.id() != val->id()) {
                FLOG_WARN("ApplyUnlock error: lock [%s] is force unlocked",
                          req.key().c_str());
                resp->mutable_resp()->set_code(LOCK_IS_FORCE_UNLOCKED);
                resp->mutable_resp()->set_error("be force unlocked");
                break;
            }
        }
        if (req.id() != val->id()) {
            FLOG_WARN("Range %" PRIu64
                      "  ApplyUnlock error: lock [%s] not locked with id %s",
                      meta_.id(), req.key().c_str(), req.id().c_str());
            resp->mutable_resp()->set_code(LOCK_ID_MISMATCHED);
            resp->mutable_resp()->set_error("wrong id: " + val->id());
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        auto btime = get_micro_second();
        ret = store_->Delete(req.key());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            FLOG_ERROR("ApplyUnlock failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("unlock failed");
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        delete val;

        FLOG_INFO("Range %" PRIu64 "  ApplyUnlock: lock [%s] is unlock by %s",
                  meta_.id(), req.key().c_str(), req.by().c_str());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        SendResponse(resp, cmd, resp->has_resp() ? resp->resp().code() : 0,
                     err);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::UnlockForce(common::ProtoMessage *msg,
                        kvrpcpb::DsUnlockForceRequest &req) {
    FLOG_DEBUG("unlock force: %s", req.DebugString().c_str());
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    auto &key = req.req().key();
    errorpb::Error *err = nullptr;
    do {
        if (!VerifyLeader(err)) {
            break;
        }
        if (!KeyInRange(key, err)) {
            break;
        }

        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::UnlockForce);
            cmd.set_allocated_unlock_force_req(req.release_req());
        });
        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] UnlockForce raft submit error: %s",
                       meta_.id(), ret.ToString().c_str());
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] UnlockForce error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new kvrpcpb::DsUnlockForceResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyUnlockForce(const raft_cmdpb::Command &cmd) {
    FLOG_DEBUG("apply unlock force: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;

    auto &req = cmd.unlock_force_req();
    auto resp = new (kvrpcpb::DsUnlockForceResponse);
    do {
        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  UnlockForce error: %s", meta_.id(),
                      err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }

        auto val = LockGet(req.key());
        if (val == nullptr) {
            FLOG_WARN("Range %" PRIu64
                      "  ApplyUnlockForce error: lock [%s] is not existed",
                      meta_.id(), req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_NOT_EXIST);
            resp->mutable_resp()->set_error("not exist");
            break;
        }
        if (val->delete_flag()) {
            FLOG_WARN("ApplyUnlock error: lock [%s] is force unlocked",
                      req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_IS_FORCE_UNLOCKED);
            resp->mutable_resp()->set_error("be force unlocked");
            break;
        }

        auto btime = get_micro_second();
        // do not really delete until the deleted time
        val->set_delete_flag(true);
        ret = store_->Put(req.key(), val->SerializeAsString());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            FLOG_ERROR("ApplyUnlockForce failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("unlock force failed");
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        delete val;

        FLOG_INFO("Range %" PRIu64
                  "  ApplyUnlockForce: lock [%s] is force unlocked by %s",
                  meta_.id(), req.key().c_str(), req.by().c_str());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        SendResponse(resp, cmd, resp->has_resp() ? resp->resp().code() : 0,
                     err);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::LockScan(common::ProtoMessage *msg, kvrpcpb::DsLockScanRequest &req) {
    FLOG_DEBUG("lock scan: %s", req.DebugString().c_str());
    context_->run_status->PushTime(monitor::PrintTag::Qwait, get_micro_second() - msg->begin_time);

    errorpb::Error *err = nullptr;
    auto ds_resp = new kvrpcpb::DsLockScanResponse;
    auto start = std::max(req.req().start(), meta_.start_key());
    auto limit = std::min(req.req().limit(), meta_.end_key());
    std::unique_ptr<storage::Iterator> iterator(store_->NewIterator(start, limit));

    int max_count = checkMaxCount(static_cast<int64_t >(req.req().count()));
    auto resp = ds_resp->mutable_resp();

    uint64_t count = 0;
    uint64_t total_size = 0;

    for (int i = 0; iterator->Valid() && i < max_count; ++i) {
        auto kv = resp->add_info();
        FLOG_DEBUG("scan key: %s", iterator->key().c_str());
        kv->set_key(std::move(iterator->key()));
        kv->mutable_value()->ParseFromString(iterator->value());

        count++;
        total_size += iterator->key().length()+iterator->value().length();

        iterator->Next();
    }

    if (resp->info_size() > 0) {
        auto lastIdx = resp->info_size()-1;
        auto lastInfo = resp->info(lastIdx);
        resp->set_last_key(lastInfo.key());
        FLOG_DEBUG("last key: %s", lastInfo.key().c_str());
    }

    context_->socket_session->SetResponseHeader(req.header(), ds_resp->mutable_header(), err);
    context_->socket_session->Send(msg, ds_resp);
}

}  // namespace range
}
}
