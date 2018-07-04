//
// Created by guo on 2/8/18.
//
#include "range.h"
#include "server/range_server.h"

namespace sharkstore {
namespace dataserver {
namespace range {

void Range::KVSet(common::ProtoMessage *msg, kvrpcpb::DsKvSetRequest &req) {
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    if (!CheckWriteable()) {
        auto resp = new kvrpcpb::DsKvSetResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    auto &key = req.req().kv().key();
    errorpb::Error *err = nullptr;

    do {
        if (!VerifyLeader(err)) {
            FLOG_WARN("range[%" PRIu64 "] KVSet error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        if (!KeyInRange(key, err)) {
            FLOG_WARN("range[%" PRIu64 "] KVSet error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        auto epoch = req.header().range_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("range[%" PRIu64 "] KvSet error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvSet);
            cmd.set_allocated_kv_set_req(req.release_req());
        });

        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] KVSet raft submit error: %s",
                       meta_.id(), ret.ToString().c_str());

            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        auto resp = new kvrpcpb::DsKvSetResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyKVSet(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;
    uint64_t affected_keys = 0;

    auto &req = cmd.kv_set_req();
    do {
        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyInsert error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        if (req.case_() != kvrpcpb::EC_Force) {
            bool bExists = store_->KeyExists(req.kv().key());
            if ((req.case_() == kvrpcpb::EC_Exists && !bExists) ||
                (req.case_() == kvrpcpb::EC_NotExists && bExists)) {
                break;
            }
            if (bExists) {
                affected_keys = 1;
            }
        }
        auto btime = get_micro_second();
        ret = store_->Put(req.kv().key(), req.kv().value());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = req.kv().key().size() + req.kv().value().size();
            CheckSplit(len);
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new kvrpcpb::DsInsertResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), affected_keys,
                     err);

    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::KVGet(common::ProtoMessage *msg, kvrpcpb::DsKvGetRequest &req) {
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    errorpb::Error *err = nullptr;
    auto ds_resp = new kvrpcpb::DsKvGetResponse;
    auto header = ds_resp->mutable_header();

    FLOG_DEBUG("range[%" PRIu64 "] KVGet begin", meta_.id());
    do {
        auto &key = req.req().key();
        if (!VerifyLeader(err)) {
            FLOG_WARN("range[%" PRIu64 "] KVGet error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }
        if (key.empty() || !KeyInRange(key, err)) {
            FLOG_WARN("range[%" PRIu64 "] KVGet error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        auto resp = ds_resp->mutable_resp();
        auto btime = get_micro_second();
        auto ret = store_->Get(req.req().key(), resp->mutable_value());

        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        resp->set_code(static_cast<int>(ret.code()));
    } while (false);

    context_->socket_session->SetResponseHeader(req.header(), header, err);
    context_->socket_session->Send(msg, ds_resp);
}

void Range::KVBatchSet(common::ProtoMessage *msg,
                       kvrpcpb::DsKvBatchSetRequest &req) {
    Status ret;
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    if (!CheckWriteable()) {
        auto resp = new kvrpcpb::DsKvBatchSetResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    errorpb::Error *err = nullptr;

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto epoch = req.header().range_epoch();
        if (!EpochIsEqual(epoch, err)) {
            break;
        }
        ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvBatchSet);
            cmd.set_allocated_kv_batch_set_req(req.release_req());
        });

        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] KVBatchSet raft submit error: %s",
                       meta_.id(), ret.ToString().c_str());

            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] KVBatchSet error: %s", meta_.id(),
                  err->message().c_str());
        auto resp = new kvrpcpb::DsKvBatchSetResponse;
        SendError(msg, req.header(), resp, RaftFailError());
    }
}

Status Range::ApplyKVBatchSet(const raft_cmdpb::Command &cmd) {
    Status ret;

    auto total_size = 0, total_count = 0;
    uint64_t affected_keys = 0;
    errorpb::Error *err = nullptr;

    do {
        auto &req = cmd.kv_batch_set_req();
        auto existCase = req.case_();

        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyKVBatchSet error: %s",
                      meta_.id(), err->message().c_str());
            break;
        }

        std::vector<std::pair<std::string, std::string>> keyValues;
        for (int i = 0, count = req.kvs_size(); i < count; ++i) {
            auto kv = req.kvs(i);
            do {
                if (req.case_() != kvrpcpb::EC_Force) {
                    bool bExists = store_->KeyExists(kv.key());
                    if ((existCase == kvrpcpb::EC_Exists && !bExists) ||
                        (existCase == kvrpcpb::EC_NotExists && bExists)) {
                        break;
                    }
                    if (bExists) {
                        ++affected_keys;
                    }
                }
                total_size += kv.key().size() + kv.value().size();
                ++total_count;

                keyValues.push_back(std::pair<std::string, std::string>(
                    std::move(kv.key()), std::move(kv.value())));
            } while (false);
        }

        auto btime = get_micro_second();
        ret = store_->BatchSet(keyValues);
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyKVBatchSet failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            CheckSplit(total_size);
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new kvrpcpb::DsKvBatchSetResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), affected_keys,
                     err);

    } else if (err != nullptr) {
        delete err;
    }

    return ret;
}

void Range::KVBatchGet(common::ProtoMessage *msg,
                       kvrpcpb::DsKvBatchGetRequest &req) {
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    errorpb::Error *err = nullptr;
    auto ds_resp = new kvrpcpb::DsKvBatchGetResponse;
    auto header = ds_resp->mutable_header();
    auto total_time = 0L;

    uint64_t count = 0;
    uint64_t total_size = 0;
    auto keys_size = req.req().keys_size();

    for (int i = 0; i < keys_size; ++i) {
        auto &key = req.req().keys(i);
        if (key.empty() || !KeyInRange(key)) {
            FLOG_WARN("range[%" PRIu64 "] KVBatchGet error: %s not in range",
                      meta_.id(), key.c_str());
        } else {
            auto kv = ds_resp->mutable_resp()->add_kvs();
            auto btime = get_micro_second();
            auto ret = store_->Get(key, kv->mutable_value());
            kv->set_key(std::move(key));
            count++;
            total_size += kv->key().size() + kv->value().size();
            total_time += get_micro_second() - btime;
        }
    }

    context_->run_status->PushTime(monitor::PrintTag::Store, total_time);

    context_->socket_session->SetResponseHeader(req.header(), header, err);
    context_->socket_session->Send(msg, ds_resp);
}

void Range::KVDelete(common::ProtoMessage *msg,
                     kvrpcpb::DsKvDeleteRequest &req) {
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    if (!CheckWriteable()) {
        auto resp = new kvrpcpb::DsKvDeleteResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

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
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvDelete);
            cmd.set_allocated_kv_delete_req(req.release_req());
        });
        if (!ret.ok()) {
            FLOG_ERROR("range[%" PRIu64 "] KVDelete raft submit error: %s",
                       meta_.id(), ret.ToString().c_str());
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] KVDelete error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new kvrpcpb::DsKvDeleteResponse;
        SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyKVDelete(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;

    do {
        auto &req = cmd.kv_delete_req();

        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyKVBatchSet error: %s",
                      meta_.id(), err->message().c_str());
            break;
        }

        auto btime = get_micro_second();
        ret = store_->Delete(req.key());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyKVDelete failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new kvrpcpb::DsKvDeleteResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), err);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::KVBatchDelete(common::ProtoMessage *msg,
                          kvrpcpb::DsKvBatchDeleteRequest &req) {
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);
    errorpb::Error *err = nullptr;

    if (!CheckWriteable()) {
        auto resp = new kvrpcpb::DsKvBatchDeleteResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    if (!VerifyLeader(err)) {
        FLOG_WARN("range[%" PRIu64 "] Insert error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new kvrpcpb::DsKvBatchDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }

    auto epoch = req.header().range_epoch();
    if (!EpochIsEqual(epoch, err)) {
        FLOG_WARN("range[%" PRIu64 "] Insert error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new kvrpcpb::DsKvBatchDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }

    for (int i = 0, count = req.req().keys_size(); i < count; ++i) {
        auto &key = req.req().keys(i);
        if (!KeyInRange(key)) {
            auto resp = new kvrpcpb::DsKvBatchDeleteResponse;
            return SendError(msg, req.header(), resp, KeyNotInRange(key));
        }
    }

    auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
        cmd.set_cmd_type(raft_cmdpb::CmdType::KvBatchDel);
        cmd.set_allocated_kv_batch_del_req(req.release_req());
    });

    if (!ret.ok()) {
        FLOG_ERROR("range[%" PRIu64 "] Insert raft submit error: %s",
                   meta_.id(), ret.ToString().c_str());

        auto resp = new kvrpcpb::DsKvBatchDeleteResponse;
        SendError(msg, req.header(), resp, RaftFailError());
    }
}

Status Range::ApplyKVBatchDelete(const raft_cmdpb::Command &cmd) {
    Status ret;

    uint64_t affected_keys = 0;
    errorpb::Error *err = nullptr;

    do {
        auto &req = cmd.kv_batch_del_req();
        std::vector<std::string> delKeys(req.keys_size());

        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyKVBatchDelete error: %s",
                      meta_.id(), err->message().c_str());
            break;
        }

        for (int i = 0, count = req.keys_size(); i < count; ++i) {
            auto &key = req.keys(i);
            if (req.case_() == kvrpcpb::EC_Exists ||
                req.case_() == kvrpcpb::EC_AnyCase) {
                if (store_->KeyExists(key)) {
                    ++affected_keys;
                    delKeys.push_back(std::move(key));
                }
            } else {
                delKeys.push_back(std::move(key));
            }
        }

        auto btime = get_micro_second();
        ret = store_->BatchDelete(delKeys);
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyKVBatchDelete failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }

    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new kvrpcpb::DsKvBatchDeleteResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), affected_keys,
                     err);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

void Range::KVRangeDelete(common::ProtoMessage *msg,
                          kvrpcpb::DsKvRangeDeleteRequest &req) {
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    if (!CheckWriteable()) {
        auto resp = new kvrpcpb::DsKvRangeDeleteResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    errorpb::Error *err = nullptr;

    if (!VerifyLeader(err)) {
        FLOG_WARN("range[%" PRIu64 "] KVRangeDelet error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new kvrpcpb::DsKvRangeDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }

    auto epoch = req.header().range_epoch();
    if (!EpochIsEqual(epoch, err)) {
        FLOG_WARN("range[%" PRIu64 "] KVRangeDelet error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new kvrpcpb::DsKvRangeDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }

    auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
        cmd.set_cmd_type(raft_cmdpb::CmdType::KvRangeDel);
        cmd.set_allocated_kv_range_del_req(req.release_req());
    });

    if (!ret.ok()) {
        FLOG_ERROR("range[%" PRIu64 "] KVRangeDelet raft submit error: %s",
                   meta_.id(), ret.ToString().c_str());

        auto resp = new kvrpcpb::DsKvRangeDeleteResponse;
        SendError(msg, req.header(), resp, RaftFailError());
    }
}

Status Range::ApplyKVRangeDelete(const raft_cmdpb::Command &cmd) {
    Status ret;

    uint64_t affected_keys = 0;
    errorpb::Error *err = nullptr;
    std::string last_key;

    do {
        auto &req = cmd.kv_range_del_req();
        auto start = std::max(req.start(), meta_.start_key());
        auto limit = std::min(req.limit(), meta_.end_key());

        auto epoch = cmd.verify_epoch();
        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("range[%" PRIu64 "] KVRangeDelet error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        auto btime = get_micro_second();
        if (req.case_() == kvrpcpb::EC_Exists ||
            req.case_() == kvrpcpb::EC_AnyCase) {
            std::unique_ptr<storage::Iterator> iterator(
                store_->NewIterator(start, limit));
            int maxCount = checkMaxCount(req.max_count());
            std::vector<std::string> delKeys(maxCount);

            for (int i = 0; iterator->Valid() && i < maxCount; ++i) {
                delKeys.push_back(std::move(iterator->key()));
                iterator->Next();
            }

            if (delKeys.size() > 0) {
                last_key = delKeys[delKeys.size() - 1];
            }

            ret = store_->BatchDelete(delKeys);
            affected_keys = delKeys.size();
        } else {
            ret = store_->RangeDelete(start, limit);
        }

        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new kvrpcpb::DsKvRangeDeleteResponse;
        resp->mutable_resp()->set_last_key(last_key);

        SendResponse(resp, cmd, static_cast<int>(ret.code()), affected_keys,
                     nullptr);
    } else if (err != nullptr) {
        delete err;
    }

    return ret;
}

void Range::KVScan(common::ProtoMessage *msg, kvrpcpb::DsKvScanRequest &req) {
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   get_micro_second() - msg->begin_time);

    errorpb::Error *err = nullptr;
    auto ds_resp = new kvrpcpb::DsKvScanResponse;
    auto start = std::max(req.req().start(), meta_.start_key());
    auto limit = std::min(req.req().limit(), meta_.end_key());
    std::unique_ptr<storage::Iterator> iterator(
        store_->NewIterator(start, limit));

    int max_count = checkMaxCount(req.req().max_count());
    auto resp = ds_resp->mutable_resp();

    uint64_t count = 0;
    uint64_t total_size = 0;

    for (int i = 0; iterator->Valid() && i < max_count; ++i) {
        auto kv = resp->add_kvs();
        kv->set_key(std::move(iterator->key()));
        kv->set_value(std::move(iterator->value()));
        iterator->Next();

        count++;
        total_size += kv->key().length() + kv->value().length();
    }

    if (resp->kvs_size() > 0) {
        resp->set_last_key(resp->kvs(resp->kvs_size() - 1).key());
    }

    context_->socket_session->SetResponseHeader(req.header(),
                                                ds_resp->mutable_header(), err);
    context_->socket_session->Send(msg, ds_resp);
}
}
}
}  // for namespace
