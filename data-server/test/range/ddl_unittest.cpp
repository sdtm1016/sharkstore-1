#include <gtest/gtest.h>
#include "cpp_permission.h"

#include <fastcommon/shared_func.h>

#include "base/status.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "range/range.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "storage/store.h"

#include "proto/gen/schpb.pb.h"

#include "raft_server_mock.h"
#include "socket_session_mock.h"

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

char level[8] = "debug";

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;

class DdlTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_init2();
        set_log_level(level);

        strcpy(ds_config.rocksdb_config.path, "/tmp/sharkstore_ds_store_test_");
        strcat(ds_config.rocksdb_config.path, std::to_string(getticks()).c_str());

        range_server_ = new server::RangeServer;

        context_ = new server::ContextServer;

        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->socket_session = new SocketSessionMock;
        context_->raft_server = new RaftServerMock;

        range_server_->Init(context_);
    }

    void TearDown() override {
        delete context_->range_server;
        delete context_->socket_session;
        delete context_->raft_server;
        delete context_;
    }

protected:
    server::ContextServer *context_;
    server::RangeServer *range_server_;
};

metapb::Range *genRange() {
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

    return meta;
}

TEST_F(DdlTest, Ddl) {
    {
        // begin test create range
        auto msg = new common::ProtoMessage;
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange());

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->CreateRange(msg);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->find(1) != nullptr);

        std::vector<std::string> metas;
        auto ret = range_server_->meta_store_->GetAllRange(metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test create range
    }

    {
        // begin test create range (repeat)
        auto msg = new common::ProtoMessage;
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange());

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->CreateRange(msg);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->find(1) != nullptr);

        schpb::CreateRangeResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // test meta_store
        std::vector<std::string> metas;
        auto ret = range_server_->meta_store_->GetAllRange(metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // end test create range
    }

    {
        // begin test create range (repeat but epoch stale)
        auto msg = new common::ProtoMessage;
        schpb::CreateRangeRequest req;
        auto meta = genRange();
        meta->mutable_range_epoch()->set_version(2);
        req.set_allocated_range(meta);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->CreateRange(msg);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->find(1) != nullptr);

        schpb::CreateRangeResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_range());

        // test meta_store
        std::vector<std::string> metas;
        auto ret = range_server_->meta_store_->GetAllRange(metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test create range
    }

    {
        // begin test delete range
        auto msg = new common::ProtoMessage;
        schpb::DeleteRangeRequest req;
        req.set_range_id(1);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->DeleteRange(msg);

        ASSERT_TRUE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->find(1) == nullptr);

        schpb::DeleteRangeResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // test meta_store
        std::vector<std::string> metas;
        auto ret = range_server_->meta_store_->GetAllRange(metas);

        ASSERT_TRUE(metas.size() == 0) << metas.size();
        // end test delete range
    }

    {
        // begin test delete range (not exist)
        auto msg = new common::ProtoMessage;
        schpb::DeleteRangeRequest req;
        req.set_range_id(1);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->DeleteRange(msg);

        schpb::DeleteRangeResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        // end test delete range
    }
}
