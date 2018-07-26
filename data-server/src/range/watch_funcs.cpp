#include "range.h"
#include "server/range_server.h"

namespace sharkstore {
namespace dataserver {
namespace range {

void Range::WatchGet(common::ProtoMessage *msg, watchpb::DsWatchRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    auto ds_resp = new watchpb::DsWatchResponse;
    auto header = ds_resp->mutable_header();
    std::string dbKey{""};
    std::string dbValue{""};
    int64_t version{0};

    //to do 暂不支持前缀watch
    auto prefix = req.req().prefix();

    FLOG_DEBUG("range[%" PRIu64 "] WatchGet begin", meta_.id());

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        if( Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchGet, meta_, req.mutable_req()->mutable_kv(), dbKey, dbValue, err) ) {
            break;
        }

        FLOG_DEBUG("range[%" PRIu64 " %s-%s] WatchGet key:%s", meta_.id(),  EncodeToHexString(meta_.start_key()).c_str(), EncodeToHexString(meta_.end_key()).c_str(), EncodeToHexString(dbKey).c_str());
        
        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(dbKey);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(dbKey);
                break;
            }
        }

        auto btime = get_micro_second();
        
        //get from rocksdb
        auto ret = store_->Get(dbKey, &dbValue);
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if(ret.ok()) {
            //decode value and response to client 
            auto resp = ds_resp->mutable_resp();
            resp->set_watchid(msg->session_id);
            //resp->set_code(Status::kOk);
            resp->set_code(static_cast<int>(ret.code()));

            //auto val = std::make_shared<std::string>();
            //auto ext = std::make_shared<std::string>();
            auto evt = resp->add_events();
            auto tmpKv = req.mutable_req()->mutable_kv();
            //decode value
            if(Status::kOk != WatchCode::DecodeKv(funcpb::kFuncWatchGet, meta_, tmpKv, dbKey, dbValue, err)) {
                break;
            }

            evt->set_type(watchpb::PUT);
            auto respKv = evt->mutable_kv();
            respKv->CopyFrom(*tmpKv);
            version = respKv->version();
            FLOG_WARN("range[%" PRIu64 "] WatchGet version: [%" PRIu64 "]", meta_.id(), version);
        } else {
            version = 0;
            FLOG_WARN("range[%" PRIu64 "] WatchGet code_: %s", meta_.id(), ret.ToString().c_str());
        }


    } while (false);

    static int16_t watchFlag{0};
    watchFlag = 0;

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] WatchGet error: %s", meta_.id(),
                  err->message().c_str());
    } else {

        //add watch if client version is not equal to ds side
        auto start_version = req.req().startversion();
        if(req.req().longpull() > 0) {
            uint64_t expireTime = getticks();
            expireTime += req.req().longpull(); 

            msg->expire_time = expireTime;
        }
        //decode version from value
        FLOG_DEBUG("range[%" PRIu64 "] (%" PRIu64 " >= %" PRIu64 "?) WatchGet [%s]-%s ok.", 
                   meta_.id(), start_version, version, EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbValue).c_str());
        if(start_version >= version) {
            //to do add watch
            AddKeyWatcher(dbKey, msg);
            watchFlag = 1;
        } else {
            FLOG_DEBUG("range[%" PRIu64 "] no watcher(%d) and will send response to client.", meta_.id(), watchFlag); 
        }
    }
    if(!watchFlag || err != nullptr) {
        context_->socket_session->SetResponseHeader(req.header(), header, err);
        context_->socket_session->Send(msg, ds_resp);
    }
}

void Range::PureGet(common::ProtoMessage *msg, watchpb::DsKvWatchGetMultiRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    auto ds_resp = new watchpb::DsKvWatchGetMultiResponse;
    auto header = ds_resp->mutable_header();
    //encode key and value
    std::string dbKey{""};
    std::string dbKeyEnd{""};
    std::string dbValue("");
    //int64_t version{0};
    int64_t minVersion(0);
    int64_t maxVersion(0);
    auto prefix = req.prefix();

    FLOG_DEBUG("range[%" PRIu64 "] PureGet begin", meta_.id());

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.kv().key();
        if (key.empty()) {
            FLOG_WARN("range[%" PRIu64 "] PureGet error: key empty", meta_.id());
            err = KeyNotInRange("EmptyKey");
            break;
        }

        //encode key
        if( 0 != WatchCode::EncodeKv(funcpb::kFuncWatchGet, meta_, req.mutable_kv(), dbKey, dbValue, err)) {
            break;
        }

        FLOG_WARN("range[%" PRIu64 "] PureGet key before:%s after:%s", meta_.id(), key[0].c_str(), EncodeToHexString(dbKey).c_str());

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(dbKey);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(dbKey);
                break;
            }
        }

        auto resp = ds_resp;
        auto btime = get_micro_second();
        storage::Iterator *it = nullptr;
        Status::Code code = Status::kOk;

        if (prefix) {
            dbKeyEnd.assign(dbKey);
            if( 0 != WatchCode::NextComparableBytes(dbKey.data(), dbKey.length(), dbKeyEnd)) {
                //to do set error message
                break;
            }
            FLOG_DEBUG("range[%" PRIu64 "] PureGet key scope %s---%s", meta_.id(), EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbKeyEnd).c_str());

            //need to encode and decode
            std::shared_ptr<storage::Iterator> iterator(store_->NewIterator(dbKey, dbKeyEnd));
            uint32_t count{0};

            for (int i = 0; iterator->Valid() ; ++i) {
                count++;
                auto kv = resp->add_kvs();
                auto tmpDbValue = iterator.get()->value();
                
                if(Status::kOk != WatchCode::DecodeKv(funcpb::kFuncPureGet, meta_, kv, dbKey, tmpDbValue, err)) {
                    //break;
                    continue;
                    FLOG_DEBUG("range[%" PRIu64 "] dbvalue:%s  err:%s", meta_.id(), EncodeToHexString(tmpDbValue).c_str(), err->message().c_str());
                }
                //to judge version after decoding value and spliting version from value
                if (minVersion > kv->version()) {
                    minVersion = kv->version();
                }
                if(maxVersion < kv->version()) {
                    maxVersion = kv->version();
                }

                iterator->Next();
            }

            FLOG_DEBUG("range[%" PRIu64 "] PureGet ok:%d ", meta_.id(), count);
            code = Status::kOk;
        } else {
            auto kv = resp->add_kvs();
            
            auto ret = store_->Get(dbKey, &dbValue);
            //to do decode value version             
            FLOG_DEBUG("range[%" PRIu64 "] PureGet:%s---%s  ", meta_.id(), EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbValue).c_str());
            if(Status::kOk != WatchCode::DecodeKv(funcpb::kFuncPureGet, meta_, kv, dbKey, dbValue, err)) {
                FLOG_DEBUG("range[%" PRIu64 "] dbvalue:%s  err:%s", meta_.id(), EncodeToHexString(dbValue).c_str(), err->message().c_str());
                break;
            }
            
            FLOG_DEBUG("range[%" PRIu64 "] PureGet code:%d msg:%s ori-value:%s ", meta_.id(), code, ret.ToString().data(), kv->value().c_str());
            code = ret.code();
        }
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        resp->set_code(static_cast<int32_t>(code));
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] PureGet error: %s", meta_.id(),
                  err->message().c_str());
    }

    context_->socket_session->SetResponseHeader(req.header(), header, err);
    context_->socket_session->Send(msg, ds_resp);
}

void Range::WatchPut(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req) {
    errorpb::Error *err = nullptr;
    auto dbKey(std::make_shared<std::string>(""));
    auto dbValue(std::make_shared<std::string>(""));
    auto extPtr(std::make_shared<std::string>(""));
    int64_t version{0};

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    FLOG_DEBUG("range[%" PRIu64 "] WatchPut begin", meta_.id());

    if (!CheckWriteable()) {
        auto resp = new watchpb::DsKvWatchPutResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto kv = req.mutable_req()->mutable_kv();
        if (kv->key().empty()) {
            FLOG_WARN("range[%" PRIu64 "] WatchPut error: key empty", meta_.id());
            err = KeyNotInRange("-");
            break;
        }

        FLOG_DEBUG("range[%" PRIu64 "] WatchPut key:%s value:%s", meta_.id(), kv->key(0).c_str(), kv->value().c_str());

        //encode key
        if( 0 != version_seq_->nextId(&version)) {
            if (err == nullptr) {
                err = new errorpb::Error;
            }
            err->set_message(version_seq_->getErrMsg());
            break;
        }
        kv->set_version(version);
        FLOG_DEBUG("range[%" PRIu64 "] WatchPut key-version[%" PRIu64 "]", meta_.id(), version);

        if( Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchPut, meta_, kv, *dbKey, *dbValue, err) ) {
            break;
        }
        
        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(*dbKey);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(*dbKey);
                break;
            }
        }

        //increase key version
        kv->set_version(version);
        kv->clear_key();
        kv->add_key(*dbKey);
        kv->set_value(*dbValue);

        //raft propagate at first, propagate KV after encodding
        if (!WatchPutSubmit(msg, req)) {
            err = RaftFailError();
        }
        
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] WatchPut error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new watchpb::DsKvWatchPutResponse;
        return SendError(msg, req.header(), resp, err);
    }

}

void Range::WatchDel(common::ProtoMessage *msg, watchpb::DsKvWatchDeleteRequest &req) {
    errorpb::Error *err = nullptr;
    auto dbKey = std::make_shared<std::string>();
    auto dbValue = std::make_shared<std::string>();
    auto extPtr = std::make_shared<std::string>();

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    FLOG_DEBUG("range[%" PRIu64 "] WatchDel begin", meta_.id());

    if (!CheckWriteable()) {
        auto resp = new watchpb::DsKvWatchDeleteResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto kv = req.mutable_req()->mutable_kv();

        if (kv->key_size() < 1) {
            FLOG_WARN("range[%" PRIu64 "] WatchDel error: key empty", meta_.id());
            err = KeyNotInRange("EmptyKey");
            break;
        }
        
        if(Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchDel, meta_, kv, *dbKey, *dbValue, err)) {
            break;
        }

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(*dbKey);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(*dbKey);
                break;
            }
        }
        //set encoding value to request
        kv->clear_key();
        kv->add_key(*dbKey);
        kv->set_value(*dbValue);

        if (!WatchDeleteSubmit(msg, req)) {
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] WatchDel error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new watchpb::DsKvWatchDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }
    
}

bool Range::WatchPutSubmit(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req) {
    auto &kv = req.req().kv();

    if (is_leader_ && kv.key_size() > 0 && KeyInRange(kv.key(0))) {
        
        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvWatchPut);
            cmd.set_allocated_kv_watch_put_req(req.release_req());
        });

        return ret.ok() ? true : false;
    }

    return false;
}

bool Range::WatchDeleteSubmit(common::ProtoMessage *msg,
                            watchpb::DsKvWatchDeleteRequest &req) {
    auto &kv = req.req().kv();

    if (is_leader_ && kv.key_size() > 0 && KeyInRange(kv.key(0))) {
        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvWatchDel);
            cmd.set_allocated_kv_watch_del_req(req.release_req());
        });

        return ret.ok() ? true : false;
    }

    return false;
}

Status Range::ApplyWatchPut(const raft_cmdpb::Command &cmd) {
    Status ret;

    FLOG_DEBUG("range [%" PRIu64 "]ApplyWatchPut begin", meta_.id());
    auto &req = cmd.kv_watch_put_req();
    auto dbKey = req.kv().key(0);
    auto dbValue = req.kv().value();
    FLOG_DEBUG("ApplyWatchPut range[%" PRIu64 "] dbkey:%s dbvalue:%s", meta_.id(), EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbValue).c_str());

    errorpb::Error *err = nullptr;

    do {

        if (!KeyInRange(dbKey, err)) {
            FLOG_WARN("Apply WatchPut failed, key:%s not in range.", dbKey.data());
            ret = std::move(Status(Status::kInvalidArgument, "key not in range", ""));
            break;
        }
        //save to db
        auto btime = get_micro_second();
        ret = store_->Put(dbKey, dbValue);
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyWatchPut failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().data());
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = static_cast<uint64_t>(req.kv().ByteSizeLong());
            CheckSplit(len);
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new watchpb::DsKvWatchPutResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), err);
    } else if (err != nullptr) {
        delete err;
    }

    //notify watcher
    std::string errMsg("");
    int32_t retCnt = WatchNotify(watchpb::PUT, req.kv(), errMsg);
    if (retCnt < 0) {
        FLOG_ERROR("WatchNotify failed, ret:%d, msg:%s", retCnt, errMsg.c_str());
    } else {
        FLOG_DEBUG("WatchNotify success, count:%d, msg:%s", retCnt, errMsg.c_str());
    }
    
    return ret;
}

Status Range::ApplyWatchDel(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;

    FLOG_DEBUG("range[%" PRIu64 "] ApplyWatchDel begin", meta_.id());

    auto &req = cmd.kv_watch_del_req();

    do {
        if (!KeyInRange(req.kv().key(0), err)) {
            FLOG_WARN("ApplyWatchDel failed, epoch is changed");
            break;
        }

        auto btime = get_micro_second();
        ret = store_->Delete(req.kv().key(0));
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyWatchDel failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }
        // ignore delete CheckSplit
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new watchpb::DsKvWatchDeleteResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), err);
    } else if (err != nullptr) {
        delete err;
    }

    //notify watcher
    uint64_t version( req.kv().version());
    int32_t retCnt(0);
    std::string errMsg("");
    retCnt = WatchNotify(watchpb::DELETE, req.kv(), errMsg);
    if (retCnt < 0) {
        FLOG_ERROR("WatchNotify failed, ret:%d, msg:%s", retCnt, errMsg.c_str());
    } else {
        FLOG_DEBUG("WatchNotify success, count:%d, msg:%s", retCnt, errMsg.c_str());
    }
    
    return ret;
}

int32_t Range::WatchNotify(const watchpb::EventType evtType, const watchpb::WatchKeyValue& kv, std::string &errMsg) {
//    Status ret;
    int32_t idx{0};
    const auto &encodeKv = kv;

    std::vector<common::ProtoMessage*> vecProtoMsg;
    auto dbKey = encodeKv.key_size()>0?encodeKv.key(0):"EMPTY-KEY";
    auto dbValue = encodeKv.value();
    
    if(dbKey == "EMPTY-KEY") {
        errMsg.assign("WatchNotify--key is empty.");
        return -1;
    }

    std::string key{""};
    std::string value{""};
    errorpb::Error *err = nullptr;

    
    uint32_t watchCnt = GetKeyWatchers(vecProtoMsg, dbKey);
    if (watchCnt > 0) {
        //to do 遍历watcher 发送通知
        auto ds_resp = new watchpb::DsWatchResponse;
        auto resp = ds_resp->mutable_resp();
        resp->set_code(Status::kOk);

        auto evt = resp->add_events();
        evt->set_type(evtType);
        //to do decode kv before send
        auto decodeKv = new watchpb::WatchKeyValue;
        decodeKv->CopyFrom(encodeKv);

    //    evt->set_allocated_kv(tmpKv.get());
        funcpb::FunctionID funcId;
        if (watchpb::PUT == evtType) {
            funcId = funcpb::kFuncWatchPut;
        } else if (watchpb::DELETE == evtType) {
            funcId = funcpb::kFuncWatchDel;
        } else {
            funcId = funcpb::kFuncWatchPut;
        }

        if( Status::kOk != WatchCode::DecodeKv(funcId, meta_, decodeKv, dbKey, dbValue, err)) {
            errMsg.assign("WatchNotify--Decode key:");
            errMsg.append(dbKey);
            errMsg.append(" fail.");

            return -1;
        }
        evt->set_allocated_kv(decodeKv);

        for(auto pMsg : vecProtoMsg) {
            if(pMsg == nullptr) {
                FLOG_WARN("WatchNotify...protoMessage pointer is null,skip notify.");
                continue;
            }
            idx++;
            FLOG_DEBUG("range[%" PRIu64 "] WatchPut-Notify[key][%s] (%" PRId32"/%" PRIu32")>>>[session][%" PRId64"]",
                       meta_.id(), EncodeToHexString(dbKey).c_str(), idx, watchCnt, pMsg->session_id);

            resp->set_watchid(pMsg->session_id);

            context_->socket_session->Send(pMsg, ds_resp);
            {
                //delete watch
                if (WATCH_OK != DelKeyWatcher(pMsg->session_id, key)) {
                    FLOG_WARN("range[%" PRIu64 "] WatchPut-Notify DelKeyWatcher WARN:[key][%s] (%" PRId32"/%" PRIu32")>>>[session][%" PRId64"]",
                           meta_.id(), key.c_str(), idx, watchCnt, pMsg->session_id);
                }
            }
        }
    } else {
        idx = 0;
        errMsg.assign("no watcher");
        FLOG_WARN("range[%" PRIu64 "] WatchPut-Notify key:%s has no watcher.",
                           meta_.id(), EncodeToHexString(dbKey).c_str());

    }

    return idx;

}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore