#include <gtest/gtest.h>
#include "helper/cpp_permission.h"

#include <fastcommon/shared_func.h>
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
#include "range/range.h"

#include "watch/watcher.h"
#include "common/socket_base.h"
#include <vector>

//#define private public

//extern void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);

#define VEC_SIZE  10000
char level[8] = "warn";

int main(int argc, char *argv[]) {
    if(argc > 1)
        strcpy(level, argv[1]);

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


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

metapb::Range *genRange2();
metapb::Range *genRange1();

class WatchTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_init2();
        set_log_level(level);

        log_set_time_precision(&g_log_context, LOG_TIME_PRECISION_MSECOND);

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

        range_server_->Init(context_);
        now = getticks();

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

        for(int32_t i=0; i++<VEC_SIZE;) {
            vec_.push_back(i);
        }

    }

    void TearDown() override {
        DestroyDB(ds_config.rocksdb_config.path, rocksdb::Options());

        delete context_->range_server;
        delete context_->socket_session;
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
    }

    void justPut(const int16_t &rangeId, const std::string &key1, const std::string &key2,const std::string &value)
    {
        FLOG_DEBUG("justPut...range:%d key1:%s  key2:%s  value:%s", rangeId, key1.c_str(), key2.c_str() , value.c_str());

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->setLeaderFlag(true);

        // begin test watch_get (ok)
        auto msg1 = new common::ProtoMessage;
        //put first
        msg1->expire_time = get_micro_second() + 1000000;
        msg1->session_id = 1;
        auto msgId(rand());
        FLOG_DEBUG("msg_id:%" PRId32, msgId);
        msg1->msg_id = msgId;
        //msg1->msg_id = 20180813;
        msg1->socket = &socket_;
        msg1->begin_time = get_micro_second();

        watchpb::DsKvWatchPutRequest req1;

        req1.mutable_header()->set_range_id(rangeId);
        req1.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req1.mutable_header()->mutable_range_epoch()->set_version(1);

        req1.mutable_req()->mutable_kv()->add_key(key1);
        if(!key2.empty())
            req1.mutable_req()->mutable_kv()->add_key(key2);
        req1.mutable_req()->mutable_kv()->set_value(value);

        auto len1 = req1.ByteSizeLong();
        msg1->body.resize(len1);
        ASSERT_TRUE(req1.SerializeToArray(msg1->body.data(), len1));

        range_server_->WatchPut(msg1);
        watchpb::DsKvWatchPutResponse resp1;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp1));
        FLOG_DEBUG("watch_put first response: %s", resp1.DebugString().c_str());

        return;
    }

    void justDel(const int16_t &rangeId, const std::string &key1, const std::string &key2, const std::string &value, bool prefix = false)
    {
        FLOG_DEBUG("justDel...range:%d key1:%s  key2:%s", rangeId, key1.c_str(), key2.c_str() );

        // begin test watch_delete( ok )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 3000;
        msg->session_id = 1;
        msg->socket = &socket_;
        msg->begin_time = get_micro_second();
        auto msgId(rand());
        FLOG_DEBUG("msg_id:%" PRId32, msgId);
        msg->msg_id = msgId;
        //msg->msg_id = 20180813;

        watchpb::DsKvWatchDeleteRequest req;

        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key(key1);
        if(!key2.empty())
            req.mutable_req()->mutable_kv()->add_key(key2);

        req.mutable_req()->mutable_kv()->set_version(1);

        req.mutable_req()->set_prefix(prefix);

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

    void justGet(const int16_t &rangeId, const std::string key1, const std::string &key2, const std::string& val, const int32_t& cnt, bool prefix = false)
    {
        FLOG_DEBUG("justGet...range:%d key1:%s  key2:%s  value:%s", rangeId, key1.c_str(), key2.c_str() , val.c_str());

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->setLeaderFlag(true);

        // begin test pure_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->begin_time = get_micro_second();
        msg->session_id = 1;
        watchpb::DsKvWatchGetMultiRequest req;

        req.set_prefix(prefix);
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_kv()->set_version(0);
        req.mutable_kv()->set_tableid(1);

        req.mutable_kv()->add_key(key1);
        if(!key2.empty())
            req.mutable_kv()->add_key(key2);


        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->PureGet(msg);

        watchpb::DsKvWatchGetMultiResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);

        //cnt为０时，仍返回编码ＯＫ　　kvs_size() == 0
        ASSERT_TRUE(session_mock->GetResult(&resp));

        FLOG_DEBUG(">>>PureGet RESP:%s", resp.DebugString().c_str());

        ASSERT_FALSE(resp.header().has_error());
        EXPECT_TRUE(resp.kvs_size() == cnt);

        if (cnt && resp.kvs_size()) {
            for(auto i = 0; i<cnt; i++)
                EXPECT_TRUE(resp.kvs(i).value() == val);
        }
    }

    void justWatch(const int16_t &rangeId, const std::string key1, const std::string key2, const int64_t version = 0, bool prefix = false)
    {
        FLOG_DEBUG("justWatch...range:%d key1:%s  key2:%s  prefix:%d", rangeId, key1.c_str(), key2.c_str(), prefix );
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->is_leader_ = true;

        // begin test watch_get (key empty)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 3000;
        msg->session_id = 1;
        msg->socket = &socket_;
        msg->begin_time = get_micro_second();
        msg->msg_id = 20180813;

        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key(key1);
        if(!key2.empty()) {
            req.mutable_req()->mutable_kv()->add_key(key2);
        }
        req.mutable_req()->mutable_kv()->set_version(1);
        req.mutable_req()->set_longpull(5000);
        ///////////////////////////////////////////////
        req.mutable_req()->set_startversion(version);
        req.mutable_req()->set_prefix(prefix);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);

        //ASSERT_TRUE(session_mock->GetResult(&resp));
        //FLOG_DEBUG("watch_get RESP:%s", resp.DebugString().c_str());
        ASSERT_FALSE(resp.header().has_error());
        //ASSERT_TRUE(resp.header().error().has_key_not_in_range());

    }

    std::thread trd1;
    std::thread trd2;
    std::condition_variable cond_;
    std::mutex mutex_;

    std::vector<int64_t> vec_;
    std::vector<std::string> str_vec_;
    std::atomic<int32_t> cnt_;

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
    meta->set_table_id(1);
    //meta->set_start_key("01003");
    //meta->set_end_key("01004");
    meta->set_start_key(keyStart);
    meta->set_end_key(keyEnd);

    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    //peer = meta->add_peers();
    //peer->set_id(2);
    //peer->set_node_id(2);

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

    return meta;
}

TEST_F(WatchTest, watch_delete_single) {

    justPut(1, "01003001", "", "03003001:value");
    justGet(1, "01003001", "", "03003001:value", 1);

    justDel(1, "01003001", "", "");
    justGet(1, "01003001", "", "", 0);

    justGet(1, "0100300101", "", "", 0);

}

TEST_F(WatchTest, watch_delete_group) {

    //插入３个　　期望get 3个
    justPut(1, "01003001", "0100300101", "03003001:value");
    justPut(1, "01003001", "0100300102", "03003001:value");
    justPut(1, "01003001", "0100300103", "03003001:value");
    justGet(1, "01003001", "", "03003001:value", 3, true);

    //前缀删除３个
    justDel(1, "01003001", "", "", true);
    //get 0个
    justGet(1, "01003001", "", "", 0, true);


}

//多线程环境: 10000次put 10000次get/delete 测试
#define watch_delete_single
#ifdef watch_put_and_get_single
TEST_F(WatchTest, watch_delete_single_multithread) {

    trd1 = std::thread([this]() {
        static bool brkFlag(false);
        do {
            std::unique_lock<std::mutex> lock( mutex_ );

            int32_t element(0);
            if (!vec_.empty()) {
                element = vec_.back();

                FLOG_DEBUG("thread1>>>%" PRId32, element);

                if (element % 2 == 0) {
                    vec_.pop_back();
                    justPut(1, "01003001", "", "03003001:value");

                    cnt_.fetch_add(1);
                    //cond_.wait(lock);
                    cond_.notify_one();
                } else {
                    //cond_.wait_for(lock, std::chrono::milliseconds(1000));
                    cond_.wait(lock);
                }

            } else {
                brkFlag = true;
            }

        }while(!brkFlag);

    });

    trd2 = std::thread([this]() {
        static bool brkFlag(false);
        do {
            std::unique_lock<std::mutex> lock(mutex_);

            int32_t element(0);
            if (!vec_.empty()) {
                element = vec_.back();

                FLOG_DEBUG("thread2>>>%" PRId32, element);

                if (element % 2 != 0) {
                    vec_.pop_back();

                    justGet(1, "01003001", "", "03003001:value", 1);
                    justDel(1, "01003001", "", "");

                    cnt_.fetch_add(1);
                    cond_.notify_one();
                } else {
                    //cond_.wait_for(lock, std::chrono::milliseconds(1000));
                    cond_.wait(lock);
                }
            } else {
                brkFlag = true;
            }

        }while(!brkFlag);

    });

    trd1.join();
    trd2.join();

    int64_t cnt = cnt_;

    FLOG_WARN("end execute times:%" PRId64, cnt);

}
#endif

TEST_F(WatchTest, watch_del_single_watch) {

//        // begin test watch_put group (key ok)
//        FLOG_DEBUG("watch_put group mode.");
//        metapb::Range* rng = new metapb::Range;
//        range_server_->meta_store_->GetRange(1, rng);
//        FLOG_DEBUG("RANGE1  %s---%s", EncodeToHexString(rng->start_key()).c_str(), EncodeToHexString(rng->end_key()).c_str());
//
//        range_server_->meta_store_->GetRange(2, rng);
//        FLOG_DEBUG("RANGE2  %s---%s", EncodeToHexString(rng->start_key()).c_str(), EncodeToHexString(rng->end_key()).c_str());

    for(auto i = 0; i < 20; i ++) {
        char szKey2[1000] = {0};
        sprintf(szKey2, "01004001%d", i);
        std::string key2(szKey2);
        //justPut(2, "01004001", key2, "01004001:value");
        justPut(2, "0100400101", key2, "01004001:value");

        auto &version(i);
        justWatch(2, "0100400101", "", version, true);

        //触发Notify version>30时
        justDel(2, "0100400101", key2, "");
    }

}

#define watch_timeout_test
#ifdef watch_timeout_test
TEST_F(WatchTest, watch_timeout_test) {

    std::string strKeyPrefix("01003001");
    str_vec_.clear();
    for(int64_t i=0; i++< VEC_SIZE;) {
        char tmp[50] = {0};
        sprintf(tmp, "%s_%ld", strKeyPrefix.c_str(), i);
        str_vec_.push_back(std::string(tmp));
    }

    trd1 = std::thread([this]() {
        static bool brkFlag(false);
        do {
            std::unique_lock<std::mutex> lock( mutex_ );

            if (!str_vec_.empty()) {
                auto element = str_vec_.back();
                auto id = atoll(element.substr(element.find("_")+1).c_str());

                if (id % 2 == 0) {
                    str_vec_.pop_back();

                    auto key = element.substr(0, element.find("_"));

                    FLOG_DEBUG("thread1>>>%s   key:%s  id:%lld" , element.c_str(), key.c_str(), id);

                    if(id%10 == 0)
                        justPut(1, "01003001", key, "03003001:value");

                    auto version = range_server_->Find(1)->apply_index_;
                    justWatch(1, key, "", version, true);

                    cnt_.fetch_add(1);
                    //cond_.wait(lock);
                    cond_.notify_one();
                } else {
                    //cond_.wait_for(lock, std::chrono::milliseconds(1000));
                    cond_.wait(lock);
                }

            } else {
                brkFlag = true;
            }

        }while(!brkFlag);

    });

    trd2 = std::thread([this]() {
        static bool brkFlag(false);
        do {
            std::unique_lock<std::mutex> lock(mutex_);

            if (!str_vec_.empty()) {

                auto element = str_vec_.back();
                auto id = atoll(element.substr(element.find("_")+1).c_str());

                if (id % 2 != 0) {
                    str_vec_.pop_back();

                    auto key = element.substr(0, element.find("_"));
                    FLOG_DEBUG("thread2>>>%s   key:%s  id:%lld" , element.data(), key.data(), id);

                    if(id%5 == 0)
                        justDel(1, "01003001", key, "", true);

                    cnt_.fetch_add(1);
                    cond_.notify_one();
                } else {
                    //cond_.wait_for(lock, std::chrono::milliseconds(1000));
                    cond_.wait(lock);
                }
            } else {
                brkFlag = true;
            }

        }while(!brkFlag);

    });

    trd1.join();
    trd2.join();

    int64_t cnt = cnt_;

    FLOG_DEBUG("end>>>>>>>>>>>cnt:%" PRId64, cnt);


    sleep(30);

}
#endif


TEST_F(WatchTest, watch_del_benchmark) {

    int64_t bTime(getticks());
    int64_t  count(1000000);

    for (int i = 0; i < count; i++) {
        justDel(1, "01003001", "", "", false);
    }
    int64_t endTime(getticks());
    FLOG_WARN("count:%" PRId64 " elapse:%" PRId64 "s average:%" PRId64 "/s",count, (endTime - bTime)/1000, 10000000/ ((endTime - bTime)/1000));

}