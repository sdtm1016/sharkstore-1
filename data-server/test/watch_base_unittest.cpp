#include <gtest/gtest.h>
#define private public

#include "range/watch.h"
#include "frame/sf_util.h"


int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {
using namespace sharkstore::dataserver;

class TestWatcherSet: public range::WatcherSet{
public:
    void CheckAddWatcher(const std::string& watcher_key, int64_t watcher_id) {
        // in key index
        auto k_idx = key_index_.find(watcher_key);
        ASSERT_TRUE(k_idx != key_index_.end());

        auto watchers = k_idx->second;
        auto watcher_it = watchers->find(watcher_id);
        ASSERT_TRUE(watcher_it != watchers->end());
        (void)watcher_it;

        // in watch index
        auto w_idx = watcher_index_.find(watcher_id);
        ASSERT_TRUE(w_idx != watcher_index_.end());

        auto keys = w_idx->second;
        auto key = keys->find(watcher_key);
        ASSERT_TRUE(key != keys->end());
        (void)key;

        // in timer queue
        auto timer_queue = timer_.GetQueue();
        std::cout << "timer get queue size: " << timer_queue.size() << std::endl;
        std::cout << "timer pri queue size: " << timer_.size() << std::endl;
        auto timer_it = timer_queue.begin();
        for (; timer_it != timer_queue.end(); ++timer_it) {
            if (timer_it->msg_->session_id == watcher_id && (timer_it->key_) == watcher_key) {
                break;
            }
        }
        ASSERT_TRUE(timer_it != timer_queue.end());
    }

    void CheckDelWatcher(const std::string& watcher_key, int64_t watcher_id) {
        // not in key index
        auto k_idx = key_index_.find(watcher_key);
        if (k_idx != key_index_.end()) {
            auto watchers = k_idx->second;
            auto watcher_it = watchers->find(watcher_id);
            ASSERT_TRUE(watcher_it == watchers->end());
        }

        // not in watch index
        auto w_idx = watcher_index_.find(watcher_id);
        if (w_idx != watcher_index_.end()) {
            auto keys = w_idx->second;
            auto key = keys->find(watcher_key);
            ASSERT_TRUE(key == keys->end());
        }

        // in timer queue
        auto timer_queue = timer_.GetQueue();
        auto timer_it = timer_queue.begin();
        for (; timer_it != timer_queue.end(); ++timer_it) {
            ASSERT_FALSE(timer_it->msg_->session_id == watcher_id && (timer_it->key_) == watcher_key);
        }
        ASSERT_TRUE(timer_it == timer_queue.end());
    }
};

TEST(TestWatcherSet, Base) {
    TestWatcherSet ws;

    common::ProtoMessage msg;
    msg.session_id = 1;
    msg.begin_time = getticks();
    msg.expire_time = getticks()+3000;
    msg.msg_id = 100;

    std::string key("testkey");
    // add watcher
    auto add_ret = ws.AddWatcher(key, &msg);
    ASSERT_EQ(0, add_ret);

    ws.CheckAddWatcher(key, 1);

    // del watcher
    auto del_ret = ws.DelWatcher(1L, key);
    //ASSERT_EQ(0, (int)del_ret);
    //ws.CheckDelWatcher(key, 1);
}

TEST(TestTimer, Base) {
    using namespace range;
    TimerQueue<Watcher> timer;

    common::ProtoMessage m1;
    m1.expire_time = 10;
    std::string k1("key1");

    common::ProtoMessage m2;
    m2.expire_time = 5;
    std::string k2("key2");

    Watcher w1;
    w1.msg_ = &m1;
    w1.key_ = k1;
    Watcher w2;
    w2.msg_ = &m2;
    w2.key_ = k2;
    timer.push(w1);
    timer.push(w2);

    auto queue = timer.GetQueue();
    ASSERT_TRUE(queue.size() == 2);

    auto i = 0;
//    for (auto it = queue.begin(); it != queue.end(); ++it) {
    for (auto it: queue) {
        if (i == 0) { ASSERT_TRUE(it.key_ == k2 && it.msg_->expire_time == 5); }
        if (i == 1) { ASSERT_TRUE(it.key_ == k1 && it.msg_->expire_time == 10); }
        ++i;
    }
}

TEST(TestEncodeDecodeKey, Base) {
//    void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);
//    bool DecodeWatchKey(std::vector<std::string*>& keys, std::string* buf);
    using namespace range;
    std::string  *buf = new std::string;
    uint64_t table_id = 1;
    std::vector<std::string*> keys;
    //key:01004001 value:01004001:value
    keys.push_back(new std::string("01004001"));
    EncodeWatchKey(buf, 1, keys);
   
    std::cout << EncodeToHexString(*buf).c_str() << std::endl;
   
    std::vector<std::string*> de_keys;
    DecodeWatchKey(de_keys, buf);
    ASSERT_TRUE(de_keys.size() == 1);
    ASSERT_TRUE(*(de_keys[0]) == std::string("01004001"));
}
/*
TEST(TestEncodeDecodeValue, Base) {
//    void EncodeWatchValue(std::string *buf,
//                          int64_t &version,
//                          const std::string *value,
//                          const std::string *extend);
//    bool DecodeWatchValue(int64_t *version, std::string *value, std::string *extend,
//                          std::string &buf);
    using namespace range;
    std::string buf;

    int64_t ver = 123;
    auto v = std::string("value123");
    auto ext = std::string("extend123");
    EncodeWatchValue(&buf, ver, &v, &ext);

    int64_t de_ver;
    std::string de_v;
    std::string de_ext;
    DecodeWatchValue(&de_ver, &de_v, &de_ext, buf);
    ASSERT_EQ(de_ver, 123);
    ASSERT_EQ(de_v, v);
    ASSERT_EQ(de_ext, ext);
}
*/
} /* namespace  */
