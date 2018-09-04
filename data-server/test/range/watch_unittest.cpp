#include <gtest/gtest.h>
#include "helper/cpp_permission.h"

#include <fastcommon/shared_func.h>
#include <common/ds_config.h>
#include "base/status.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "proto/gen/watchpb.pb.h"
#include "proto/gen/schpb.pb.h"
#include "range/range.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "storage/store.h"

#include "helper/mock/raft_server_mock.h"
#include "helper/mock/socket_session_mock.h"
#include "helper/table.h"
#include "range/range.h"

#include "watch/watcher.h"
#include "common/socket_base.h"

//extern void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);


int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


char level[8] = "warn";

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::range;
using namespace sharkstore::dataserver::storage;

class SocketBaseMock: public common::SocketBase {

public:
    virtual int Send(response_buff_t *response) {
        FLOG_DEBUG("Send mock...%s", response->buff);
        return 0;
    }
};


std::string  DecodeSingleKey(const int16_t grpFlag, const std::string &encodeBuf) {
    std::vector<std::string *> vec;
    std::string key("");
    auto buf = new std::string(encodeBuf);

    watch::Watcher watcher(1, vec);
    watcher.DecodeKey(vec, encodeBuf);

        if(grpFlag) {
            for(auto it:vec) {
                key.append(*it);
            }
        } else {
            key.assign(*vec[0]);
        }

    
   //     FLOG_DEBUG("DecodeWatchKey exception(%d), %s", int(vec.size()), EncodeToHexString(*buf).c_str());
    

    if(vec.size() > 0 && key.empty())
        key.assign(*vec[0]);

    FLOG_DEBUG("DecodeKey: %s", key.c_str());
    return key;
}

class WatchTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_init2();
        set_log_level(level);

        strcpy(ds_config.rocksdb_config.path, "/tmp/sharkstore_ds_store_test_");
        strcat(ds_config.rocksdb_config.path, std::to_string(getticks()).c_str());

        sf_socket_thread_config_t config;
        sf_socket_status_t status = {0};

        socket_.Init(&config, &status);

        range_server_ = new server::RangeServer;

        context_ = new server::ContextServer;

        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->socket_session = new SocketSessionMock;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        ds_config.range_config.recover_concurrency = 1;
        range_server_->Init(context_);
        now = getticks();
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
    int64_t now;
    SocketBaseMock socket_;
};

metapb::Range *genRange1() {
    //watch::Watcher watcher;
    auto meta = new metapb::Range;
    
    std::vector<std::string*> keys;
    keys.clear();
    std::string keyStart("");
    std::string keyEnd("");
    std::string k1("01003"), k2("01004");

    keys.push_back(&k1);
    watch::Watcher watcher1(1, keys);
    watcher1.EncodeKey(&keyStart, 1, keys);

    keys.clear();
    keys.push_back(&k2);
    watch::Watcher watcher2(1, keys);
    watcher2.EncodeKey(&keyEnd, 1, keys);

    meta->set_id(1);
    //meta->set_start_key("01003");
    //meta->set_end_key("01004");
    meta->set_start_key(keyStart);
    meta->set_end_key(keyEnd);

    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    peer = meta->add_peers();
    peer->set_id(2);
    peer->set_node_id(2);

    auto pks = sharkstore::test::helper::CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

metapb::Range *genRange2() {
    //watch::Watcher watcher;
    auto meta = new metapb::Range;

    std::vector<std::string*> keys;
    keys.clear();
    std::string keyStart("");
    std::string keyEnd("");
    std::string k1("01004"), k2("01005");

    keys.push_back(&k1);
    watch::Watcher watcher1(1, keys);
    watcher1.EncodeKey(&keyStart, 1, keys);

    keys.clear();
    keys.push_back(&k2);
    watch::Watcher watcher2(1, keys);
    watcher2.EncodeKey(&keyEnd, 1, keys);

    meta->set_id(2);
    //meta->set_start_key("01004");
    //meta->set_end_key("01005");
    meta->set_start_key(keyStart);
    meta->set_end_key(keyEnd);

    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    auto pks = sharkstore::test::helper::CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}
/*
TEST_F(WatchTest, watch_put_no_leader) {
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
        // begin test watch_put (no leader)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        range_server_->ranges_[1]->setLeaderFlag(false);
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        if (resp.header().has_error()) {
            FLOG_WARN("has error:%s", resp.header().error().message().c_str());
        }
        EXPECT_TRUE(resp.header().has_error());
        EXPECT_TRUE(resp.header().error().has_not_leader());
        //ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        //ASSERT_TRUE(resp.header().error().message() == "no leader");

        // end test watch_put
    }
}


TEST_F(WatchTest, watch_put_not_leader) {
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
        // begin test watch_put (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(false);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test watch_put
    }
}

TEST_F(WatchTest, watch_put_not_in_range) {
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
        // begin test watch_put (not in range)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01004001");
        req.mutable_req()->mutable_kv()->set_value("01004001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());
        std::string key1("");
        key1 = DecodeSingleKey(0, resp.header().error().key_not_in_range().start_key());
        FLOG_DEBUG("encodekey:%s   decodekey:%s ", resp.header().error().key_not_in_range().start_key().c_str(),
                   key1.c_str());

        ASSERT_TRUE(key1 == "01003");
        //ASSERT_TRUE(resp.header().error().key_not_in_range().end_key() == "01004");
        std::string key2 = DecodeSingleKey(0, resp.header().error().key_not_in_range().end_key());

        ASSERT_TRUE(key2 == "01004");

        // end test watch_put
    }
}

*/
TEST_F(WatchTest, watch_put_group_get_group) {
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
        // begin test watch_put group (key ok)
        FLOG_DEBUG("watch_put group mode.");

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->add_key("01003002");
        req.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // end test watch_put
    }

    //test get group

    FLOG_DEBUG("pure_get group..." );
    {
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        // begin test pure_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->msg_id = 2018080301;
        watchpb::DsKvWatchGetMultiRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_kv()->add_key("01003001");
        req.mutable_kv()->add_key("01003002");
        req.mutable_kv()->set_version(0);
        //req.mutable_kv()->set_tableid(1);

        //will print in range_server
        //FLOG_DEBUG("pureGet req:%s", req.DebugString().c_str());

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->PureGet(msg);

        watchpb::DsKvWatchGetMultiResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        FLOG_DEBUG("pureGet group RESP:%s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.kvs(0).value() == "01003001:value");

    }
}

TEST_F(WatchTest, watch_put_group_del_get_nothing) {
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
        // begin test watch_put group (key ok)
        FLOG_DEBUG("watch_put group mode.");

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->add_key("01003002");
        req.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // end test watch_put
    }

    //delete group
    {
        // begin test watch_delete( ok )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->socket = &socket_;
        watchpb::DsKvWatchDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->add_key("01003002");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchDel(msg);

        watchpb::DsKvWatchDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        FLOG_DEBUG("watch_del response: %s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());

        // end test watch_delete
    }


    //test get group

    FLOG_DEBUG("pure_get group...nothign" );
    {
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        // begin test pure_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->msg_id = 2018080301;
        watchpb::DsKvWatchGetMultiRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_kv()->add_key("01003001");
        req.mutable_kv()->add_key("01003002");
        req.mutable_kv()->set_version(0);
        //req.mutable_kv()->set_tableid(1);

        //will print in range_server
        //FLOG_DEBUG("pureGet req:%s", req.DebugString().c_str());

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->PureGet(msg);

        watchpb::DsKvWatchGetMultiResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        FLOG_DEBUG("pureGet group RESP:%s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());
        if(resp.kvs_size())
            ASSERT_TRUE(resp.kvs(0).value() != "01003001:value");

    }
}


TEST_F(WatchTest, watch_put_single) {
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
        // begin test watch_put (ok)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 100;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // end test watch_put
    }

}

///
//TEST_F(WatchTest, watch_put_stale_epoch) {
//    {
//        // begin test create range
//        auto msg = new common::ProtoMessage;
//        schpb::CreateRangeRequest req;
//        req.set_allocated_range(genRange1());
//
//        auto len = req.ByteSizeLong();
//        msg->body.resize(len);
//        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));
//
//        range_server_->CreateRange(msg);
//        ASSERT_FALSE(range_server_->ranges_.empty());
//
//        ASSERT_TRUE(range_server_->Find(1) != nullptr);
//
//        std::vector<metapb::Range> metas;
//        auto ret = range_server_->meta_store_->GetAllRange(&metas);
//
//        ASSERT_TRUE(metas.size() == 1) << metas.size();
//        // end test create range
//    }
//
//    {
//        // begin test create range
//        auto msg = new common::ProtoMessage;
//        schpb::CreateRangeRequest req;
//        req.set_allocated_range(genRange2());
//
//        auto len = req.ByteSizeLong();
//        msg->body.resize(len);
//        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));
//
//        range_server_->CreateRange(msg);
//        ASSERT_FALSE(range_server_->ranges_.empty());
//
//        ASSERT_TRUE(range_server_->Find(2) != nullptr);
//
//        std::vector<metapb::Range> metas;
//        auto ret = range_server_->meta_store_->GetAllRange(&metas);
//
//        ASSERT_TRUE(metas.size() == 2) << metas.size();
//        // end test create range
//    }
//
//    {
//        // begin test watch_put (ok, retry split range)
//
//        // set leader
//        range_server_->ranges_[1]->split_range_id_ = 2;
//        {
//            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
//            raft->ops_.leader = 1; raft->SetLeaderTerm(1, 1);
//            range_server_->ranges_[1]->setLeaderFlag(true);
//        }
//
//        {
//            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
//            raft->ops_.leader = 1; raft->SetLeaderTerm(1, 1);
//            range_server_->ranges_[2]->setLeaderFlag(true);
//        }
//
//        auto msg = new common::ProtoMessage;
//        msg->expire_time = getticks() + 1000;
//        watchpb::DsKvWatchPutRequest req;
//
//        req.mutable_header()->set_range_id(1);
//        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
//        req.mutable_header()->mutable_range_epoch()->set_version(2);
//
//        req.mutable_req()->mutable_kv()->add_key("01004001");
//        req.mutable_req()->mutable_kv()->set_value("01004001:value");
//
//        auto len = req.ByteSizeLong();
//        msg->body.resize(len);
//        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));
//
//        range_server_->WatchPut(msg);
//
//        watchpb::DsKvWatchPutResponse resp;
//        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
//        ASSERT_TRUE(session_mock->GetResult(&resp));
//
//        //ASSERT_FALSE(resp.header().has_error());
//        ASSERT_FALSE(resp.header().has_error());
//
//        // end test watch_put
//    }
//
//}

TEST_F(WatchTest, pure_get_ok) {

    range_server_->DeleteRange(1);
    range_server_->DeleteRange(2);
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

    std::cout << "pure_get ...ok" << '\n';
    {
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[2]->setLeaderFlag(true);

        metapb::Range* rng = new metapb::Range;
        range_server_->meta_store_->GetRange(1, rng);
        FLOG_DEBUG("RANGE1  %s---%s", EncodeToHexString(rng->start_key()).c_str(), EncodeToHexString(rng->end_key()).c_str());

        range_server_->meta_store_->GetRange(2, rng);
        FLOG_DEBUG("RANGE2  %s---%s", EncodeToHexString(rng->start_key()).c_str(), EncodeToHexString(rng->end_key()).c_str());


        //put first
        auto msg1 = new common::ProtoMessage;
        //put first
        msg1->expire_time = getticks() + 1000;
        msg1->session_id = 1;
        msg1->socket = &socket_;
        watchpb::DsKvWatchPutRequest req1;

        req1.mutable_header()->set_range_id(2);
        req1.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req1.mutable_header()->mutable_range_epoch()->set_version(1);

        req1.mutable_req()->mutable_kv()->add_key("01004001");
        req1.mutable_req()->mutable_kv()->set_value("01004001:value");
        req1.mutable_req()->mutable_kv()->set_tableid(1);

        auto len1 = req1.ByteSizeLong();
        msg1->body.resize(len1);
        ASSERT_TRUE(req1.SerializeToArray(msg1->body.data(), len1));

        //auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
        //raft->ops_.leader = 1;
        // raft->SetLeaderTerm(1, 1);
        //range_server_->ranges_[2]->setLeaderFlag(true);

        range_server_->WatchPut(msg1);


        // begin test pure_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        watchpb::DsKvWatchGetMultiRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_kv()->add_key("01004001");
        req.mutable_kv()->set_version(0);
        req.mutable_kv()->set_tableid(1);

        FLOG_DEBUG("RESP:%s", req.DebugString().c_str());

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->PureGet(msg);

        watchpb::DsKvWatchGetMultiResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        FLOG_DEBUG("RESP:%s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.kvs(0).value() == "01004001:value");

        }

}


TEST_F(WatchTest, pure_get_group) {
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

    FLOG_DEBUG("pure_get ...group ok");
    {
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        // begin test watch_get (ok)
        auto msg1 = new common::ProtoMessage;
        //put first
        msg1->expire_time = getticks() + 1000;
        msg1->session_id = 1;
        msg1->socket = &socket_;
        watchpb::DsKvWatchPutRequest req1;

        req1.mutable_header()->set_range_id(1);
        req1.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req1.mutable_header()->mutable_range_epoch()->set_version(1);

        req1.mutable_req()->mutable_kv()->add_key("01003001");
        req1.mutable_req()->mutable_kv()->add_key("01003002");
        req1.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len1 = req1.ByteSizeLong();
        msg1->body.resize(len1);
        ASSERT_TRUE(req1.SerializeToArray(msg1->body.data(), len1));

        range_server_->WatchPut(msg1);
        watchpb::DsKvWatchPutResponse resp1;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp1));
        FLOG_DEBUG("watch_put first response: %s", resp1.DebugString().c_str());


        //auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        //raft->ops_.leader = 1;
        // raft->SetLeaderTerm(1, 1);
        //range_server_->ranges_[1]->setLeaderFlag(true);

        // begin test pure_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchGetMultiRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_kv()->add_key("01003001");
        req.mutable_kv()->add_key("01003002");
        req.mutable_kv()->set_version(-1);
        req.mutable_kv()->set_tableid(1);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->PureGet(msg);

        watchpb::DsKvWatchGetMultiResponse resp;
        ASSERT_TRUE(session_mock->GetResult(&resp));

        FLOG_INFO("pureGet response:%s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.kvs(0).value() == "01003001:value");
    }

}

TEST_F(WatchTest, pure_get_prefix) {
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

    FLOG_DEBUG("pure_get ...prefix ok");
    {
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        // begin test pure_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchGetMultiRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_kv()->add_key("01003001");
        req.mutable_kv()->set_version(-1);
        req.mutable_kv()->set_tableid(1);
        req.set_prefix(true);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->PureGet(msg);

        watchpb::DsKvWatchGetMultiResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        if (resp.kvs_size()) { ;
            ASSERT_TRUE(resp.kvs(0).value() == "01003001:value");
        } else {
            FLOG_DEBUG("no kvs_size");
        }
        FLOG_DEBUG("watch_get response: %s", resp.DebugString().c_str());
    }
}


TEST_F(WatchTest, watch_get_again) {
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

    FLOG_DEBUG("watch_get ... ok");
    {
        // begin test watch_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->socket = &socket_;

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->set_version(-1);
        req.mutable_req()->set_longpull(now + 5000);
        req.mutable_req()->set_startversion(100000);


        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));


        FLOG_WARN("error:%s", resp.header().error().message().c_str());
        ASSERT_FALSE(resp.header().has_error());
        if (resp.resp().events_size()) { ;
            ASSERT_TRUE(resp.resp().events(0).kv().value() == "01003001:value");
        }

        FLOG_DEBUG("watch_get response: %s", resp.DebugString().c_str());
        // end test watch_get
    }


    FLOG_DEBUG("watch_get group...ok");
    {
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        // begin test watch_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->socket = &socket_;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->add_key("01003002");
        req.mutable_req()->mutable_kv()->set_version(-1);
        req.mutable_req()->set_longpull(now + 5000);

        req.mutable_req()->set_startversion(100000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        //不能确定一定有返回结果　需要参数预期配合此断言
        //ASSERT_TRUE(session_mock->GetResult(&resp));
        //FLOG_DEBUG("watch_get response: %s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());
        if (resp.resp().events_size()) { ;
            ASSERT_TRUE(resp.resp().events(0).kv().value() == "01003001:value");
        }

        FLOG_DEBUG("watch_get response: %s", resp.resp().DebugString().c_str());
        // end test watch_get
    }

    std::cout << "watch_get ...ok" << '\n';
    {
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[2]->setLeaderFlag(true);

        // begin test watch_get (ok)
        auto msg1 = new common::ProtoMessage;
        //put first
        msg1->expire_time = getticks() + 1000;
        msg1->session_id = 1;
        msg1->socket = &socket_;
        watchpb::DsKvWatchPutRequest req1;

        req1.mutable_header()->set_range_id(2);
        req1.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req1.mutable_header()->mutable_range_epoch()->set_version(1);

        req1.mutable_req()->mutable_kv()->add_key("01004001");
        req1.mutable_req()->mutable_kv()->set_value("01004001:value");

        auto len1 = req1.ByteSizeLong();
        msg1->body.resize(len1);
        ASSERT_TRUE(req1.SerializeToArray(msg1->body.data(), len1));

        range_server_->WatchPut(msg1);
        watchpb::DsKvWatchPutResponse resp1;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp1));
        FLOG_DEBUG("watch_put first response: %s", resp1.DebugString().c_str());

        //get next
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->socket = &socket_;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01004001");
        req.mutable_req()->mutable_kv()->set_version(-1);
        req.mutable_req()->set_longpull(now + 5000);
        req.mutable_req()->set_startversion(100000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        session_mock->GetResult(&resp);
        if(resp.has_resp())
            FLOG_DEBUG("watch_get second response: %s", resp.DebugString().c_str());

        //ADDWATCHER SUCCESS
        ASSERT_FALSE(resp.header().has_error());
        // end test watch_get
    }

}

TEST_F(WatchTest, watch_delete) {
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

    std::cout << "watch_get ...key empty" << '\n';
    {
        // begin test watch_get (key empty)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->socket = &socket_;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("");
        req.mutable_req()->mutable_kv()->set_version(-1);
        req.mutable_req()->set_longpull(now + 5000);
        req.mutable_req()->set_startversion(100000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        FLOG_DEBUG("watch_get RESP:%s", resp.DebugString().c_str());
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        FLOG_DEBUG("watch_get response: %s", resp.DebugString().c_str());
        // end test watch_get
    }

    std::cout << "watch_get 2...ensure not to be deleted" << '\n';
    {
        // begin test watch_get( ensure not to be deleted )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[2]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->socket = &socket_;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01004001");
        req.mutable_req()->set_longpull(now + 5000);
        req.mutable_req()->set_startversion(100);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        //ASSERT_TRUE(session_mock->GetResult(&resp));
        //FLOG_DEBUG("watch_get response: %s", resp.DebugString().c_str());
        //ASSERT_TRUE(resp.header().has_error());
        if (resp.resp().events_size())
            ASSERT_TRUE(resp.resp().events(0).kv().value() == "01004001:value");

        // end test watch_get
    }


    {
        // begin test watch_delete( ok )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->socket = &socket_;
        watchpb::DsKvWatchDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchDel(msg);

        watchpb::DsKvWatchDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        FLOG_DEBUG("watch_del response: %s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());

        // end test watch_delete
    }

    {
        // begin test watch_delete (ok, retry split range)

        // set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
            raft->ops_.leader = 1;
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[1]->setLeaderFlag(true);
        }

        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
            raft->ops_.leader = 1;
            raft->SetLeaderTerm(1, 1);
            range_server_->ranges_[2]->setLeaderFlag(true);
        }

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->socket = &socket_;
        watchpb::DsKvWatchDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->mutable_kv()->add_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchDel(msg);

        watchpb::DsKvWatchDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        FLOG_DEBUG("watch_del response: %s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());

        // end test watch_delete
    }

    {
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        // begin test watch_get(ensure watch delete)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->socket = &socket_;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->set_longpull(now + 5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        session_mock->GetResult(&resp);
        if(resp.has_resp())
            FLOG_DEBUG("watch_get response: %s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());
        if (resp.resp().events_size())
            ASSERT_TRUE(resp.resp().events(0).kv().value().empty());

        // end test watch_get
    }

    {
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[2]->setLeaderFlag(true);

        // begin test watch_get (ensure watch delete)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        msg->socket = &socket_;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01004001");
        req.mutable_req()->mutable_kv()->set_version(1);
        req.mutable_req()->set_longpull(now + 5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        session_mock->GetResult(&resp);
        if(resp.has_resp())
            FLOG_DEBUG("watch_get response: %s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());
        if (resp.resp().events_size())
            ASSERT_TRUE(resp.resp().events(0).kv().value().empty());

        // end test watch_get
    }

    FLOG_DEBUG("watch_unittest [%s].", "ok");
}



   /* ////////////////////////////////////////////////////////////////////////ignore
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
    */




