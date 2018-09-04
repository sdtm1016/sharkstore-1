#include "range_test_fixture.h"

#include <fastcommon/shared_func.h>
#include "storage/meta_store.h"
#include "base/util.h"

#include "helper_util.h"
#include "helper/table.h"
#include "helper/mock/socket_session_mock.h"
#include "helper/mock/raft_mock.h"

namespace sharkstore {
namespace test {
namespace helper {

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::common;
using namespace google::protobuf;

void RangeTestFixture::SetUp() {
    log_init2();
    char level[] = "CRIT";
    set_log_level(level);

    table_ = CreateAccountTable();

    context_.reset(new mock::RangeContextMock);
    auto s = context_->Init();
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto meta = MakeRangeMeta(table_.get(), 3);
    s = context_->CreateRange(meta, 0, 0, &range_);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

void RangeTestFixture::TearDown() {
    if (context_) {
        context_->Destroy();
    }
}

ProtoMessage* NewMsg(const Message& req) {
    auto msg = new common::ProtoMessage;
    msg->begin_time = getticks();
    msg->expire_time = getticks() + 1000;
    msg->session_id = randomInt();
    msg->header.msg_id = randomInt();
    msg->msg_id = msg->header.msg_id;

    auto len = req.ByteSizeLong();
    msg->body.resize(len);
    req.SerializeToArray(msg->body.data(), len);

    return msg;
}

void RangeTestFixture::MakeHeader(RequestHeader *header, uint64_t version) {
    header->set_range_id(range_->id_);
    if (version == 0) {
        header->mutable_range_epoch()->set_version(range_->meta_.GetVersion());
    } else {
        header->mutable_range_epoch()->set_version(version);
    }
}

void RangeTestFixture::SetLeader(uint64_t leader) {
    ++term_;
    auto r = std::static_pointer_cast<RaftMock>(range_->raft_);
    r->SetLeaderTerm(leader, term_);
    range_->is_leader_ = (leader == range_->node_id_);
}

Status RangeTestFixture::Split() {
    // 测试，只允许分裂一次
    if (range_->split_range_id_ != 0) {
        return Status(Status::kDuplicate, "split", "already take place");
    }


    mspb::AskSplitResponse resp;

    std::string split_key;
    EncodeKeyPrefix(&split_key, table_->GetID());
    split_key.push_back('\x70');
    resp.set_split_key(split_key);

    range_->split_range_id_ = 2;
    resp.set_new_range_id(range_->split_range_id_);
    range_->meta_.Get(resp.mutable_range());
    for (const auto &peer : resp.range().peers()) {
        resp.add_new_peer_ids(peer.id() + 1);
    }

    auto old_end_key = range_->meta_.GetEndKey();
    auto old_ver = range_->meta_.GetVersion();

    range_->AdminSplit(resp);

    // 分裂后检查：
    // version是否+1
    if (range_->meta_.GetVersion() != old_ver + 1) {
        return Status(Status::kUnexpected, "version", std::to_string(range_->meta_.GetVersion()));
    }
    // end_key
    if (range_->meta_.GetEndKey() != split_key) {
        return Status(Status::kUnexpected, "end key", range_->meta_.GetEndKey());
    }
    // store end key
    if (range_->store_->GetEndKey() != split_key) {
        return Status(Status::kUnexpected, "store end key", range_->store_->GetEndKey());
    }
    // 新range是否创建
    auto split_range = context_->FindRange(range_->split_range_id_);
    if (split_range == nullptr) {
        return Status(Status::kNotFound, "new split range", "");
    }
    // 新range meta
    auto split_meta = split_range->options();
    if (split_meta.id() != range_->split_range_id_) {
        return Status(Status::kUnexpected, "split range id", split_meta.DebugString());
    }
    if (split_meta.end_key() != old_end_key) {
        return Status(Status::kUnexpected, "split end key", split_meta.DebugString());
    }
    if (split_meta.start_key() != split_key) {
        return Status(Status::kUnexpected, "split start key", split_meta.DebugString());
    }
    // 新range peer
    auto meta = range_->meta_.Get();
    if (split_meta.peers_size() != meta.peers_size()) {
        return Status(Status::kUnexpected, "split peer size", split_meta.DebugString());
    }
    for (int i = 0; i < split_meta.peers_size(); ++i) {
        const auto &old_peer = meta.peers(i);
        const auto &new_peer = split_meta.peers(i);
        if (old_peer.id() + 1 != new_peer.id()) {
            return Status(Status::kUnexpected, "split peer id", split_meta.DebugString());
        }
        if (old_peer.node_id() != new_peer.node_id()) {
            return Status(Status::kUnexpected, "split peer node id", split_meta.DebugString());
        }
        if (new_peer.type() != metapb::PeerType_Normal) {
            return Status(Status::kUnexpected, "split peer type", split_meta.DebugString());
        }
    }
    // 主键
    if (split_meta.primary_keys_size() != meta.primary_keys_size()) {
        return Status(Status::kUnexpected, "split pk size", split_meta.DebugString());
    }
    for (int i = 0; i < split_meta.primary_keys_size(); ++i) {
        const auto &old_pk = meta.primary_keys(i);
        const auto &new_pk = split_meta.primary_keys(i);
        if (old_pk.ShortDebugString() != new_pk.ShortDebugString()) {
            return Status(Status::kUnexpected, "pk", split_meta.DebugString());
        }
    }

//    // 检查meta store
//    std::vector<metapb::Range> metas;
//    auto s = context_->MetaStore()->GetAllRange(&metas);
//    if (!s.ok()) {
//        return s;
//    }
//    if (metas.size() != 2) {
//        return Status(Status::kUnexpected, "meta storage size", std::to_string(metas.size()));
//    }
//    if (metas[0].ShortDebugString() != meta.ShortDebugString()) {
//        return Status(Status::kUnexpected, "meta storage", metas[0].ShortDebugString());
//    }
//    if (metas[1].ShortDebugString() != split_meta.ShortDebugString()) {
//        return Status(Status::kUnexpected, "meta storage", metas[1].ShortDebugString());
//    }

//    std::cout << split_meta.DebugString() << std::endl;

    return Status::OK();
}

Status RangeTestFixture::getResult(google::protobuf::Message *resp) {
    auto session_mock = dynamic_cast<SocketSessionMock*>(context_->SocketSession());
    if (!session_mock->GetResult(resp)) {
        return Status(Status::kUnexpected, "could not get result", "");
    } else {
        return Status::OK();
    }
}

Status RangeTestFixture::TestInsert(DsInsertRequest &req, DsInsertResponse *resp) {
    auto msg = NewMsg(req);
    range_->Insert(msg, req);
    return getResult(resp);
}

Status RangeTestFixture::TestSelect(DsSelectRequest& req, DsSelectResponse* resp) {
    auto msg = NewMsg(req);
    range_->Select(msg, req);
    return getResult(resp);
}

Status RangeTestFixture::TestDelete(DsDeleteRequest& req, DsDeleteResponse* resp) {
    auto msg = NewMsg(req);
    range_->Delete(msg, req);
    return getResult(resp);
}

void RangeTestFixture::SetLogLevel(char *level) {
    set_log_level(level);
}


} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
