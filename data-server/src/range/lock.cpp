#include "range.h"

#include "base/util.h"
#include "server/range_server.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {
namespace lock {

void EncodeKey(std::string* buf,
               uint64_t tableId, const std::string* key) {
    assert(buf != nullptr && buf->length() == 0);

    buf->push_back(static_cast<char>(1));
    EncodeUint64Ascending(buf, tableId); // column 1
    assert(buf->length() == 9);

    EncodeBytesAscending(buf, key->c_str(), key->length());
}

bool DecodeKey(std::string& key,
               const std::string& buf) {
    assert(buf.length() > 9);

    size_t offset;
    for (offset = 9; offset < buf.length();) {
        if (!DecodeBytesAscending(buf, offset, &key)) {
            return false;
        }
    }
    return true;
}

void EncodeValue(std::string* buf,
                          int64_t version,
                          const kvrpcpb::LockValue& lock_value, //const std::string* value,
                          const std::string* extend) {
    assert(buf != nullptr);
    std::string value = lock_value.SerializeAsString();
    EncodeIntValue(buf, 2, version);
    EncodeBytesValue(buf, 3, value.c_str(), value.length());
    EncodeBytesValue(buf, 4, extend->c_str(), extend->length());
}

bool DecodeValue(int64_t* version, kvrpcpb::LockValue* lock_value, std::string* extend,
                 std::string& buf) {
    assert(buf.length() != 0);

    std::string value;
    size_t offset = 0;

    if (!DecodeIntValue(buf, offset, version)) return false;
    if (!DecodeBytesValue(buf, offset, &value)) return false;
    if (!DecodeBytesValue(buf, offset, extend)) return false;
    if (!lock_value->ParseFromString(value)) return false;
    return true;
}

} // namespace lock

using namespace sharkstore::monitor;

kvrpcpb::LockValue *Range::LockGet(const std::string &key) {
    RANGE_LOG_DEBUG("lock get: key[%s]", EncodeToHexString(key).c_str());
    std::string val;
    if (!store_->Get(key, &val).ok()) {
        FLOG_WARN("lock get: no key[%s]", EncodeToHexString(key).c_str());
        return nullptr;
    }

    RANGE_LOG_DEBUG("lock get ok: key[%s] val[%s]", EncodeToHexString(key).c_str(),
               EncodeToHexString(val).c_str());

    auto ret = new kvrpcpb::LockValue;
    int64_t version = 0; // not used
    std::string extend(""); // not used
    if (!lock::DecodeValue(&version, ret, &extend,
                           val)) {
        RANGE_LOG_WARN("lock get: decode value failed, key[%s]", EncodeToHexString(key).c_str());
        return nullptr;
    }

    if (ret->delete_time() > 0 && ret->delete_time() <= getticks()) {
        RANGE_LOG_WARN("key[%s] deteled at time %ld", EncodeToHexString(key).c_str(),
                  ret->delete_time());
        return nullptr;
    }

    /*if (getticks() - ret->update_time() > DEFAULT_LOCK_DELETE_TIME_MILLSEC) {
        FLOG_WARN("key[%s] deteled last update time %ld > 3s",
                  EncodeToHexString(key).c_str(), ret->update_time());
        return nullptr;
    }*/

    RANGE_LOG_DEBUG("lock get parse: key[%s] val[%s]",
               EncodeToHexString(key).c_str(), ret->DebugString().c_str());
    return ret;
}

void Range::Lock(common::ProtoMessage *msg, kvrpcpb::DsLockRequest &req) {
    RANGE_LOG_DEBUG("lock request: %s", req.DebugString().c_str());
    context_->Statistics()->PushTime(HistogramType::kQWait,
			get_micro_second() - msg->begin_time);

    std::string encode_key;
    lock::EncodeKey(&encode_key, meta_.GetTableID(), &req.req().key());
    //req.mutable_req()->set_key(encode_key);

    //auto& key = req.req().key();
    errorpb::Error *err = nullptr;

    do {
        if (!VerifyLeader(err)) {
            RANGE_LOG_WARN("Lock error: %s", err->message().c_str());
            break;
        }

        if (!KeyInRange(encode_key, err)) {
            RANGE_LOG_WARN("Lock error: %s", err->message().c_str());
            break;
        }

        auto epoch = req.header().range_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("Lock error: %s", err->message().c_str());
            break;
        }

        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::Lock);
            cmd.set_allocated_lock_req(req.release_req());
        });

        if (!ret.ok()) {
            RANGE_LOG_ERROR("Lock raft submit error: %s", ret.ToString().c_str());

            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        auto resp = new kvrpcpb::DsLockResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyLock(const raft_cmdpb::Command &cmd) {
    RANGE_LOG_DEBUG("apply lock: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;
    auto atime = get_micro_second();

    auto req = cmd.lock_req();
    auto resp = new (kvrpcpb::DsLockResponse);
    do {
        auto &epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("ApplyLock error: %s", err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }

        std::string encode_key;
        lock::EncodeKey(&encode_key, meta_.GetTableID(), &req.key());

        auto val = LockGet(encode_key);
        // 允许相同id的重复执行lock
        if (val != nullptr) {
            if (req.value().id() != val->id()) {
                RANGE_LOG_WARN("ApplyLock error: lock [%s] is existed", req.key().c_str());
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

        std::string value_buf;
        int64_t version = 0;
        std::string extend("");

        lock::EncodeValue(&value_buf,
                         version, req.value(), &extend);
        ret = store_->Put(encode_key, value_buf);

        context_->Statistics()->PushTime(HistogramType::kQWait,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyLock failed, code:%d, msg:%s", ret.code(), ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("lock failed");
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = encode_key.size() + req.value().ByteSizeLong();
            CheckSplit(len);
        }
        delete val;


        RANGE_LOG_INFO("ApplyLock: lock [%s] is locked by %s", req.key().c_str(), req.value().by().c_str());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        ReplySubmit(cmd, resp, err, atime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::LockUpdate(common::ProtoMessage *msg,
                       kvrpcpb::DsLockUpdateRequest &req) {
    RANGE_LOG_DEBUG("lock update: %s", req.DebugString().c_str());
    context_->Statistics()->PushTime(HistogramType::kQWait,
                                   get_micro_second() - msg->begin_time);

    std::string encode_key;
    lock::EncodeKey(&encode_key, meta_.GetTableID(), &req.req().key());
    //req.mutable_req()->set_key(encode_key);

    //auto &key = req.req().key();
    errorpb::Error *err = nullptr;

    do {
        if (!VerifyLeader(err)) {
            RANGE_LOG_WARN("LockUpdate error: %s", err->message().c_str());
            break;
        }

        if (!KeyInRange(encode_key, err)) {
            RANGE_LOG_WARN("LockUpdate error: %s", err->message().c_str());
            break;
        }

        auto epoch = req.header().range_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("LockUpdate error: %s", err->message().c_str());
            break;
        }

        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::LockUpdate);
            cmd.set_allocated_lock_update_req(req.release_req());
        });

        if (!ret.ok()) {
            RANGE_LOG_ERROR("LockUpdate raft submit error: %s", ret.ToString().c_str());

            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        auto resp = new kvrpcpb::DsLockUpdateResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyLockUpdate(const raft_cmdpb::Command &cmd) {
    RANGE_LOG_DEBUG("apply lock update: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;
    auto atime = get_micro_second();

    auto &req = cmd.lock_update_req();
    auto resp = new (kvrpcpb::DsLockUpdateResponse);
    do {
        auto &epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("ApplyLockUpdate error: %s", err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }

        std::string encode_key;
        lock::EncodeKey(&encode_key, meta_.GetTableID(), &req.key());

        auto val = LockGet(encode_key);
        if (val == nullptr) {
            RANGE_LOG_WARN("ApplyLockUpdate error: lock [%s] is not existed", req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_NOT_EXIST);
            resp->mutable_resp()->set_error("not exist");
            break;
        }
        if (req.id() != val->id()) {
            RANGE_LOG_WARN("ApplyLockUpdate error: lock [%s] can not update with id "
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

        std::string value_buf;
        int64_t version = 0;
        std::string extend("");

        val->set_value(req.update_value());
        val->set_update_time(req.update_time());
        val->set_by(req.by());
        lock::EncodeValue(&value_buf,
                         version, *val, &extend);

        ret = store_->Put(encode_key, value_buf);
        context_->Statistics()->PushTime(HistogramType::kQWait,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyLockUpdate failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("lock update failed");
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = encode_key.size() + req.ByteSizeLong();
            CheckSplit(len);
        }
        delete val;

        RANGE_LOG_INFO("ApplyLockUpdate: lock [%s] is update", req.key().c_str());
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        ReplySubmit(cmd, resp, err, atime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::Unlock(common::ProtoMessage *msg, kvrpcpb::DsUnlockRequest &req) {
    RANGE_LOG_DEBUG("unlock: %s", req.DebugString().c_str());
    context_->Statistics()->PushTime(HistogramType::kQWait,
                                   get_micro_second() - msg->begin_time);

    std::string encode_key;
    lock::EncodeKey(&encode_key, meta_.GetTableID(), &req.req().key());
    //req.mutable_req()->set_key(encode_key);

    //auto &key = req.req().key();
    errorpb::Error *err = nullptr;
    do {
        if (!VerifyLeader(err)) {
            break;
        }
        if (!KeyInRange(encode_key, err)) {
            break;
        }

        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::Unlock);
            cmd.set_allocated_unlock_req(req.release_req());
        });
        if (!ret.ok()) {
            RANGE_LOG_ERROR("Unlock raft submit error: %s", ret.ToString().c_str());
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("Unlock error: %s", err->message().c_str());

        auto resp = new kvrpcpb::DsUnlockResponse;
        SendError(msg, req.header(), resp, err);
    }
    return;
}

Status Range::ApplyUnlock(const raft_cmdpb::Command &cmd) {
    RANGE_LOG_DEBUG("apply unlock: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;
    auto atime = get_micro_second();

    auto &req = cmd.unlock_req();
    auto resp = new (kvrpcpb::DsUnlockResponse);
    do {
        auto &epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("ApplyUnlock error: %s", err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }

        std::string encode_key;
        lock::EncodeKey(&encode_key, meta_.GetTableID(), &req.key());

        auto val = LockGet(encode_key);
        if (val == nullptr) {
            RANGE_LOG_WARN("ApplyUnlock error: lock [%s] is not existed", req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_NOT_EXIST);
            resp->mutable_resp()->set_error("not exist");
            break;
        }

        if (req.id() != val->id()) {
            RANGE_LOG_WARN("ApplyUnlock error: lock [%s] not locked with id %s", req.key().c_str(), req.id().c_str());
            resp->mutable_resp()->set_code(LOCK_ID_MISMATCHED);
            resp->mutable_resp()->set_error("wrong id: " + val->id());
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        auto btime = get_micro_second();
        ret = store_->Delete(encode_key);
        context_->Statistics()->PushTime(HistogramType::kQWait,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyUnlock failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("unlock failed");
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        delete val;

        RANGE_LOG_INFO("ApplyUnlock: lock [%s] is unlock by %s", EncodeToHexString(req.key()).c_str(), req.by().c_str());

        std::string decode_key = req.key();
        //std::string decode_key;
        //auto decode_ret = lock::DecodeKey(decode_key, req.key());
        //if (!decode_ret) {
        //    FLOG_ERROR("ApplyUnlock decode lock key [%s] failed", EncodeToHexString(req.key()).c_str());
        //}

        std::string err_msg;
        watchpb::WatchKeyValue watch_kv;

        watch_kv.add_key(decode_key);
        auto retCnt = WatchNotify(watchpb::DELETE, watch_kv, watch_kv.version(), err_msg);

        if (retCnt < 0) {
            RANGE_LOG_ERROR("ApplyUnlock WatchNotify failed, ret:%d, msg:%s", retCnt, err_msg.c_str());
        } else {
            RANGE_LOG_DEBUG("ApplyUnlock WatchNotify success, count:%d, msg:%s", retCnt, err_msg.c_str());
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        ReplySubmit(cmd, resp, err, atime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::UnlockForce(common::ProtoMessage *msg,
                        kvrpcpb::DsUnlockForceRequest &req) {
    RANGE_LOG_DEBUG("unlock force: %s", req.DebugString().c_str());
    context_->Statistics()->PushTime(HistogramType::kQWait,
                                   get_micro_second() - msg->begin_time);

    std::string encode_key;
    lock::EncodeKey(&encode_key, meta_.GetTableID(), &req.req().key());
    //req.mutable_req()->set_key(encode_key);

    //auto &key = req.req().key();
    errorpb::Error *err = nullptr;
    do {
        if (!VerifyLeader(err)) {
            break;
        }
        if (!KeyInRange(encode_key, err)) {
            break;
        }

        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::UnlockForce);
            cmd.set_allocated_unlock_force_req(req.release_req());
        });
        if (!ret.ok()) {
            RANGE_LOG_ERROR("UnlockForce raft submit error: %s", ret.ToString().c_str());
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("UnlockForce error: %s", err->message().c_str());

        auto resp = new kvrpcpb::DsUnlockForceResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyUnlockForce(const raft_cmdpb::Command &cmd) {
    RANGE_LOG_DEBUG("apply unlock force: %s", cmd.DebugString().c_str());
    Status ret;
    errorpb::Error *err = nullptr;
    auto atime = get_micro_second();

    auto &req = cmd.unlock_force_req();
    auto resp = new (kvrpcpb::DsUnlockForceResponse);
    do {
        auto &epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  UnlockForce error: %s", id_, err->message().c_str());
            resp->mutable_resp()->set_code(LOCK_EPOCH_ERROR);
            resp->mutable_resp()->set_error(err->message());
            break;
        }

        std::string encode_key;
        lock::EncodeKey(&encode_key, meta_.GetTableID(), &req.key());

        auto val = LockGet(encode_key);
        if (val == nullptr) {
            FLOG_WARN("Range %" PRIu64
                      "  ApplyUnlockForce error: lock [%s] is not existed",
                      id_, req.key().c_str());
            resp->mutable_resp()->set_code(LOCK_NOT_EXIST);
            resp->mutable_resp()->set_error("not exist");
            break;
        }

        auto btime = get_micro_second();
        ret = store_->Delete(encode_key);
        context_->Statistics()->PushTime(HistogramType::kQWait,
                                       get_micro_second() - btime);
        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyForceUnlock failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            resp->mutable_resp()->set_code(LOCK_STORE_FAILED);
            resp->mutable_resp()->set_error("force unlock failed");
            resp->mutable_resp()->set_value(val->value());
            resp->mutable_resp()->set_update_time(val->update_time());
            break;
        }
        delete val;

        RANGE_LOG_INFO("ApplyForceUnlock: lock [%s] is unlock by %s", EncodeToHexString(req.key()).c_str(), req.by().c_str());

        std::string decode_key = req.key();
        //std::string decode_key;
        //auto decode_ret = lock::DecodeKey(decode_key, req.key());
        //if (!decode_ret) {
        //    FLOG_ERROR("ApplyUnlockForce decode lock key [%s] failed", EncodeToHexString(req.key()).c_str());
        //}

        std::string err_msg;
        watchpb::WatchKeyValue watch_kv;

        watch_kv.add_key(decode_key);
        auto retCnt = WatchNotify(watchpb::DELETE, watch_kv, watch_kv.version(), err_msg);

        if (retCnt < 0) {
            FLOG_ERROR("ApplyUnlockForce WatchNotify failed, ret:%d, msg:%s", retCnt, err_msg.c_str());
        } else {
            FLOG_DEBUG("ApplyUnlockForce WatchNotify success, count:%d, msg:%s", retCnt, err_msg.c_str());
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        ReplySubmit(cmd, resp, err, atime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::LockWatch(common::ProtoMessage *msg,
                        watchpb::DsWatchRequest& req) {
    errorpb::Error *err = nullptr;
    if (req.req().kv().key_size() != 1) {
        RANGE_LOG_INFO("LockWatch: kv key size[%d] != 1", req.req().kv().key_size());

        err = new errorpb::Error;
        err->set_message("key list length != 1");
        auto resp = new watchpb::DsWatchResponse;
        resp->mutable_resp()->set_code(LOCK_PARAMETER_ERROR);
        SendError(msg, req.header(), resp, err);
        return;
    }

    std::string encode_key;
    lock::EncodeKey(&encode_key, meta_.GetTableID(), &req.req().kv().key(0));
    RANGE_LOG_INFO("LockWatch: lock watch key[%s] encode[%s]",
              req.req().kv().key(0).c_str(), EncodeToHexString(encode_key).c_str());

    do {
        if (!VerifyLeader(err)) {
            break;
        }
        if (!KeyInRange(encode_key, err)) {
            break;
        }

        auto val = LockGet(encode_key);
        if (val == nullptr) {
            FLOG_WARN("LockWatch error: lock encode key [%s] is not existed",
                      EncodeToHexString(encode_key).c_str());
            //err = new errorpb::Error;
            //err->set_message("lock is not existed");

            auto resp = new watchpb::DsWatchResponse;
            resp->mutable_resp()->set_code(LOCK_NOT_EXIST);
            SendError(msg, req.header(), resp, err);
            return;
        }

        std::vector<watch::Key*> keys;
        keys.push_back(new watch::Key(req.req().kv().key(0)));

        //int64_t expireTime = (req.req().longpull() > 0)?getticks() + req.req().longpull():msg->expire_time;
        int64_t expireTime = (req.req().longpull() > 0)?get_micro_second() + req.req().longpull()*1000:msg->expire_time*1000;

        auto w_ptr = std::make_shared<watch::Watcher>(meta_.GetTableID(), keys, 0, expireTime, msg);
        auto w_code = context_->WatchServer()->AddKeyWatcher(w_ptr, store_.get());
        if (w_code != watch::WATCH_OK) {
            FLOG_WARN("LockWatch error: lock [%s] add key watcher failed",
                      EncodeToHexString(encode_key).c_str());
            err = new errorpb::Error;
            err->set_message("add key watcher failed");
            break;
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] LockWatch error: %s", id_, err->message().c_str());

        auto resp = new watchpb::DsWatchResponse;
        SendError(msg, req.header(), resp, err);
    }
}

void Range::LockScan(common::ProtoMessage *msg, kvrpcpb::DsLockScanRequest &req) {
    FLOG_DEBUG("lock scan: %s", req.DebugString().c_str());
    context_->Statistics()->PushTime(HistogramType::kQWait, get_micro_second() - msg->begin_time);

    errorpb::Error *err = nullptr;
    auto ds_resp = new kvrpcpb::DsLockScanResponse;
    auto start = std::max(req.req().start(), start_key_);
    auto limit = std::min(req.req().limit(), meta_.GetEndKey());
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

    common::SetResponseHeader(req.header(), ds_resp->mutable_header(), err);
    context_->SocketSession()->Send(msg, ds_resp);
}

}  // namespace range
}
}
