#include <gtest/gtest.h>
#include "helper/cpp_permission.h"

#include <fastcommon/shared_func.h>
#include "base/status.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/schpb.pb.h"
#include "range/range.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "storage/store.h"

#include "helper/table.h"
#include "helper/mock/raft_server_mock.h"
#include "helper/mock/socket_session_mock.h"

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

char level[8] = "debug";

using namespace sharkstore::test::helper;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;

class RawTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_init2();
        set_log_level(level);

        strcpy(ds_config.rocksdb_config.path, "/tmp/sharkstore_ds_store_test_");
        strcat(ds_config.rocksdb_config.path, std::to_string(getticks()).c_str());
        ds_config.range_config.recover_concurrency = 1;

        range_server_ = new server::RangeServer;

        context_ = new server::ContextServer;

        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->socket_session = new SocketSessionMock;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        range_server_->Init(context_);
    }

    void TearDown() override {
        DestroyDB(ds_config.rocksdb_config.path, rocksdb::Options());

        delete context_->range_server;
        delete context_->socket_session;
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
    }

protected:
    server::ContextServer *context_;
    server::RangeServer *range_server_;
};

metapb::Range *genRange1() {
    auto meta = new metapb::Range;

    meta->set_id(1);
    meta->set_start_key("01003");
    meta->set_end_key("01004");
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    peer = meta->add_peers();
    peer->set_id(2);
    peer->set_node_id(2);

    auto pks = CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

metapb::Range *genRange2() {
    auto meta = new metapb::Range;

    meta->set_id(2);
    meta->set_start_key("01004");
    meta->set_end_key("01005");
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    auto pks = CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

TEST_F(RawTest, Raw) {
    {
        // begin test create range
        auto msg = new common::ProtoMessage;
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange1());

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->CreateRange(msg);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->Find(1) != nullptr);

        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test create range
    }

    {
        // begin test create range
        auto msg = new common::ProtoMessage;
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange2());

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->CreateRange(msg);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->Find(2) != nullptr);

        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 2) << metas.size();
        // end test create range
    }

    {
        // begin test raw_put (no leader)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");
        req.mutable_req()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawPut(msg);

        kvrpcpb::DsKvRawPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().message() == "no leader");

        // end test raw_put
    }

    {
        // begin test raw_put (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(2, 1);
        range_server_->ranges_[1]->is_leader_ = false;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");
        req.mutable_req()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawPut(msg);

        kvrpcpb::DsKvRawPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test raw_put
    }

    {
        // begin test raw_put (not in range)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01004001");
        req.mutable_req()->set_value("01004001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawPut(msg);

        kvrpcpb::DsKvRawPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());
        ASSERT_TRUE(resp.header().error().key_not_in_range().start_key() == "01003");
        ASSERT_TRUE(resp.header().error().key_not_in_range().end_key() == "01004");

        // end test raw_put
    }

    {
        // begin test raw_put (stale epoch)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->set_key("01004001");
        req.mutable_req()->set_value("01004001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawPut(msg);

        kvrpcpb::DsKvRawPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());

        // end test raw_put
    }

    {
        // begin test raw_put (key empty)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("");
        req.mutable_req()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawPut(msg);

        kvrpcpb::DsKvRawPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        // end test raw_put
    }

    {
        // begin test raw_put (ok)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");
        req.mutable_req()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawPut(msg);

        kvrpcpb::DsKvRawPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // end test raw_put
    }

    {
        // begin test raw_put (ok, retry split range)

        // set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[1]->is_leader_ = true;
        }

        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[2]->is_leader_ = true;
        }

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->set_key("01004001");
        req.mutable_req()->set_value("01004001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawPut(msg);

        kvrpcpb::DsKvRawPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // end test raw_put
    }

    {
        // begin test raw_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value() == "01003001:value");

        // end test raw_get
    }

    {
        // begin test raw_get (ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().DebugString();
        ASSERT_TRUE(resp.resp().value() == "01004001:value");

        // end test raw_get
    }

    {
        // begin test raw_get (key empty)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());
        // end test raw_get
    }

    {
        // begin test raw_get (no leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(0, 2);
        range_server_->ranges_[1]->is_leader_ = false;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().message() == "no leader");

        // end test raw_get
    }

    {
        // begin test raw_get (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(2, 2);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test raw_get
    }

    {
        // begin test raw_get (not in range)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        // end test raw_get
    }

    {
        // begin test raw_get (ok, retry split range)

        // set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[1]->is_leader_ = true;
        }

        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[2]->is_leader_ = true;
        }

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value() == "01004001:value");

        // end test raw_get
    }

    {
        // begin test raw_delete (key empty)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());
        // end test raw_delete
    }

    {
        // begin test raw_delete (no leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(0, 1);
        range_server_->ranges_[1]->is_leader_ = false;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().message() == "no leader");

        // end test raw_delete
    }

    {
        // begin test raw_delete (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(2, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test raw_delete
    }

    {
        // begin test raw_delete (not in range)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        // end test raw_delete
    }

    {
        // begin test raw_get( ensure not to be deleted )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value() == "01003001:value");

        // end test raw_get
    }

    {
        // begin test raw_get( ensure not to be deleted )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value() == "01004001:value");

        // end test raw_get
    }

    {
        // begin test raw_delete( ok )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // end test raw_delete
    }

    {
        // begin test raw_delete (ok, retry split range)

        // set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[1]->is_leader_ = true;
        }

        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[2]->is_leader_ = true;
        }

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // end test raw_delete
    }

    {
        // begin test raw_get(ensure raw delete)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value().empty());

        // end test raw_get
    }

    {
        // begin test raw_get (ensure raw delete)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value().empty());

        // end test raw_get
    }

    {
        // begin test delete range (range 1)
        auto msg = new common::ProtoMessage;
        schpb::DeleteRangeRequest req;
        req.set_range_id(1);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->DeleteRange(msg);

        ASSERT_TRUE(range_server_->Find(1) == nullptr);

        schpb::DeleteRangeResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // test meta_store
        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test delete range
    }

    {
        // begin test delete range (range 2)
        auto msg = new common::ProtoMessage;
        schpb::DeleteRangeRequest req;
        req.set_range_id(2);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->DeleteRange(msg);

        ASSERT_TRUE(range_server_->Find(2) == nullptr);

        schpb::DeleteRangeResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        // test meta_store
        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 0) << metas.size();
        // end test delete range
    }
}
