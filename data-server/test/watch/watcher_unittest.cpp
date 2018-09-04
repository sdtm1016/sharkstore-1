#include <gtest/gtest.h>

#define private public
#include "watch/watcher.h"
#include "watch/watcher_set.h"
#include "watch/watch_server.h"
#include "frame/sf_util.h"
#include "fastcommon/logger.h"
#include "storage/store.h"

int main(int argc, char* argv[]) {
    log_init2();

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::watch;

enum TestCode {
    TEST_OK = 0,
    TEST_ERR,
    TEST_TIMEOUT,
    TEST_UNEXPECTED,
};

class TestWatchConnection: public common::ProtoMessage {
public:
    TestWatchConnection() = delete;
    explicit TestWatchConnection(int timeout): timeout_(timeout+1000), ProtoMessage_(timeout) { // request timeout > server timeout 1s
        auto ret = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_);

        /*
        char *write_buf = "A";
        char read_buf[2] = {0};

        write(sock_[1], write_buf, 1);
        read(sock_[0], read_buf, 1);
        printf("test buf: %s\n", read_buf);
        */
    }
    TestWatchConnection(int timeout, const char* send_string, const char* expect_string):
            timeout_(timeout+1000), ProtoMessage_(timeout) {
        memcpy(send_string_, send_string, strlen(send_string) > sizeof(send_string_)-1 ? sizeof(send_string_)-1 : strlen(send_string));
        memcpy(expect_string_, expect_string, strlen(expect_string) > sizeof(expect_string_)-1 ? sizeof(expect_string_)-1 : strlen(expect_string));
        auto ret = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_);
    }

public:
    TestCode ClientRead() {
        std::cout << "client read ... " << std::endl;
        return Read(0);
    }
    TestCode ServerRead() {
        std::cout << "server read ... " << std::endl;
        return Read(1);
    }
    void ClientWrite() {
        std::cout << "client write ... " << std::endl;
        Write(0, send_string_);
        return;
    }
    void ServerWrite() {
        std::cout << "server write ... " << std::endl;

        auto expire = expire_time/1000;
        auto now = getticks()/1000;
        if (expire <= now) {
            std::cout << "expire time(sec): " << expire << " <= now: " << now << std::endl;
            Write(1, "timeout");
            return;
        }
        std::cout << "expire time(sec): " << expire << " > now: " << now << std::endl;
        Write(1, send_string_);
        return;
    }

private:
    TestCode Read(int role) {
//        assert(role == 0 || role == 1); // 0 = client; 1 = server;

        std::unique_lock<std::mutex> mutex(mutex_);

        std::cv_status wait_status;
        wait_status = cond_.wait_for(mutex, std::chrono::milliseconds(timeout_)) ;
        if (wait_status == std::cv_status::timeout) {
            return TEST_ERR;
        }

        /*
        auto ep_fd = epoll_create(1);

        struct epoll_event ev;
        ev.data.fd = sock_[role];
        ev.events = EPOLLIN;
        assert(epoll_ctl(ep_fd, EPOLL_CTL_ADD, sock_[role], &ev) == 0);

        auto max_events = 1;
        struct epoll_event events[max_events];
        std::cout << "epoll <" << std::endl;
        auto n = epoll_wait(ep_fd, events, max_events, timeout_);
        std::cout << ">" << std::endl;

        if (n == 0) {
            return TEST_TIMEOUT;
        }

        assert(events[0].data.fd == sock_[role]);
         */
        auto readn = read(sock_[role], recv_string_, strlen(expect_string_));
        std::cout << "read sock: " << role << " recv string: " << recv_string_ << " readn: " << readn <<
                  " expected string: " << expect_string_  << std::endl;
        if (readn == strlen(expect_string_) && strcmp(recv_string_, expect_string_) == 0) {
            if (strcmp(recv_string_, "timeout") == 0) {
                return TEST_TIMEOUT;
            } else {
                return TEST_OK;
            }
        } else {
            std::cout << "read unexpected string"<< std::endl;
            return TEST_UNEXPECTED;
        }

        /*
        close(ep_fd);
         */
    }

    void Write(int role, const char* buf) {
        std::unique_lock<std::mutex> mutex(mutex_);

        auto writen = write(sock_[role], buf, strlen(buf));
        std::cout << "write sock: " << role << " send string: " << buf << " nwrite: " << writen << std::endl;
        ASSERT_TRUE(writen == strlen(buf));

        cond_.notify_one();
    }

private:
    int timeout_;
    int sock_[2];
    char recv_string_[8] = {0};// max used 7 bytes XXX
    char send_string_[8] = "abc";
    char expect_string_[8] = {0};
    std::mutex mutex_;
    std::condition_variable cond_;
};


const uint64_t g_table_id = 123;

class TestWatcher: public Watcher {
public:
    TestWatcher() = delete;
    TestWatcher(const std::vector<Key*>& keys, TestWatchConnection* message): Watcher(g_table_id, keys, 0, message) {
    }
    ~TestWatcher() = default;

    void Send(google::protobuf::Message* resp) override;
};
typedef std::shared_ptr<TestWatcher> TestWatcherPtr;
typedef std::string Key;

void TestWatcher::Send(google::protobuf::Message* resp) {
    (void) resp; // not used
    std::lock_guard<std::mutex> lock(send_lock_);
    if (sent_response_flag) {
        return;
    }

    auto conn = dynamic_cast<TestWatchConnection*>(message_);
    if (conn) {
        conn->ServerWrite();
    }

    sent_response_flag = true;
}

class TestWatcherSet: public WatcherSet {
public:
    void CheckAddKeyWatcher(TestWatcherPtr& w_ptr) {
        WatcherKey encode_key;
        w_ptr->EncodeKey(&encode_key, g_table_id, w_ptr->GetKeys());
        auto watcher_id = w_ptr->GetWatcherId();

        // check key map
        auto watcher_id_it = key_map_.find(watcher_id);
        ASSERT_TRUE(watcher_id_it != key_map_.end());

        auto watcher_key_map = watcher_id_it->second;
        auto key_it = watcher_key_map->find(encode_key);
        ASSERT_TRUE(key_it != watcher_key_map->end());
        std::cout << "check add key map ok: session id: [" <<
                  watcher_id << "] key: [" << encode_key << "]" << std::endl;

        // check watcher map
        auto key_watcher_it = key_watcher_map_.find(encode_key);
        ASSERT_TRUE(key_watcher_it != key_watcher_map_.end());

        auto key_watcher_map = key_watcher_it->second->mapKeyWatcher;
        auto watcher_it = key_watcher_map.find(watcher_id);
        ASSERT_TRUE(watcher_it != key_watcher_map.end());
        std::cout << "check add watcher map ok: session id: [" <<
                  watcher_id << "] key: [" << encode_key << "]" << std::endl;

        // check queue
        auto watcher_queue = watcher_queue_.GetQueue();
        auto it = watcher_queue.begin();
        for (; it != watcher_queue.end(); ++it) {
            WatcherKey tmp_key;
            auto w_p = it->get();
            WatcherId tmp_id = w_p->GetWatcherId();

            w_p->EncodeKey(&tmp_key, g_table_id, w_p->GetKeys());
            if (tmp_key == encode_key && tmp_id == watcher_id) {
                std::cout << "check add watcher queue ok: session id: [" <<
                          tmp_id << "] key: [" << tmp_key << "]" << std::endl;
                break;
            }
        }
        ASSERT_TRUE(it != watcher_queue.end());
    }

    void CheckDelKeywatcher(TestWatcherPtr& w_ptr, bool is_sent_response) {
        WatcherKey encode_key;
        w_ptr->EncodeKey(&encode_key, g_table_id, w_ptr->GetKeys());
        auto watcher_id = w_ptr->GetWatcherId();

        // check key map
        auto watcher_id_it = key_map_.find(watcher_id);
        if (watcher_id_it != key_map_.end()) {
            auto watcher_key_map = watcher_id_it->second;
            auto key_it = watcher_key_map->find(encode_key);
            ASSERT_TRUE(key_it == watcher_key_map->end());
            std::cout << "check del key map ok: session id: [" <<
                      watcher_id << "] key: [" << encode_key << "]" << std::endl;
        }

        // check watcher map
        auto key_watcher_it = key_watcher_map_.find(encode_key);
        if (key_watcher_it != key_watcher_map_.end()) {
            auto key_watcher_map = key_watcher_it->second->mapKeyWatcher;
            auto watcher_it = key_watcher_map.find(watcher_id);
            ASSERT_TRUE(watcher_it == key_watcher_map.end());
            std::cout << "check del watcher map ok: session id: [" <<
                      watcher_id << "] key: [" << encode_key << "]" << std::endl;
        }

        // check queue
        auto watcher_queue = watcher_queue_.GetQueue();
        auto it = watcher_queue.begin();
        for (; it != watcher_queue.end(); ++it) {
            WatcherKey tmp_key;
            auto w_p = it->get();
            WatcherId tmp_id = w_p->GetWatcherId();

            w_p->EncodeKey(&tmp_key, g_table_id, w_p->GetKeys());
            if (tmp_key == encode_key && tmp_id == watcher_id) {
                ASSERT_TRUE(w_p->IsSentResponse() == is_sent_response);
                std::cout << "check del watcher in queue: session id: [" <<
                          tmp_id << "] key: [" << tmp_key << "] is sent response: [" << w_p->IsSentResponse() << "]" << std::endl;
                break;
            }
        }
        ASSERT_TRUE(it != watcher_queue.end()); // ptr is always in queue, until queue pop
    }

};

class TestWatchServer: public WatchServer {
public:
    TestWatchServer():WatchServer(3) {}
};

TEST(TestWatcher, EncodeAndDecode) {
    std::vector<Key*> keys;
    keys.push_back(new Key("k1"));
    keys.push_back(new Key("k2"));
    keys.push_back(new Key("k10"));

    auto* message = new TestWatchConnection(2000);

    TestWatcher w(keys, message);

    // test encode and decode key
    WatcherKey encode_key;
    w.EncodeKey(&encode_key, g_table_id, w.GetKeys());

    std::vector<Key*> decode_keys;
    w.DecodeKey(decode_keys, encode_key);
    std::cout << "0: " << *decode_keys[0] <<  std::endl;
    ASSERT_EQ(*decode_keys[0], Key("k1"));
    std::cout << "1: " << *decode_keys[1] <<  std::endl;
    ASSERT_EQ(*decode_keys[1], Key("k2"));
    std::cout << "2: " << *decode_keys[2] <<  std::endl;
    ASSERT_EQ(*decode_keys[2], Key("k10"));
}


TEST(TestWatcherSet, AddAndDelKeyWatcher) {
    std::vector<Key*> keys0;
    keys0.push_back(new Key("k0.1"));
    keys0.push_back(new Key("k0.2"));
    keys0.push_back(new Key("k0.10"));

    std::vector<Key*> keys1;
    keys1.push_back(new Key("k1.1"));
    keys1.push_back(new Key("k1.2"));
    keys1.push_back(new Key("k1.10"));

    auto* msg0 = new TestWatchConnection(2000);
//    msg0->session_id = 1;
//    msg0->expire_time = getticks()+3000;

    auto* msg1 = new TestWatchConnection(2000);
//    msg1->session_id = 2;
//    msg1->expire_time = getticks()+3000;

    TestWatcherPtr w_ptr0 = std::make_shared<TestWatcher>(keys0, msg0);
    TestWatcherPtr w_ptr1 = std::make_shared<TestWatcher>(keys0, msg1);
    TestWatcherPtr w_ptr2 = std::make_shared<TestWatcher>(keys1, msg1);

    std::string encode_key0;
    w_ptr0->EncodeKey(&encode_key0, g_table_id, w_ptr0->GetKeys());
    std::string encode_key1;
    w_ptr1->EncodeKey(&encode_key1, g_table_id, w_ptr1->GetKeys());
    std::string encode_key2;
    w_ptr2->EncodeKey(&encode_key2, g_table_id, w_ptr2->GetKeys());

    TestWatcherSet ws;
    WatcherPtr w_p0 = std::static_pointer_cast<Watcher>(w_ptr0);
    w_p0->SetWatcherId(0);
    WatcherPtr w_p1 = std::static_pointer_cast<Watcher>(w_ptr1);
    w_p1->SetWatcherId(1);
    WatcherPtr w_p2 = std::static_pointer_cast<Watcher>(w_ptr2);
    w_p2->SetWatcherId(2);

    // test add
    ws.AddKeyWatcher(encode_key0, w_p0, nullptr);
    ws.CheckAddKeyWatcher(w_ptr0);

    ws.AddKeyWatcher(encode_key1, w_p1, nullptr);
    ws.CheckAddKeyWatcher(w_ptr1);

    ws.AddKeyWatcher(encode_key2, w_p2, nullptr);
    ws.CheckAddKeyWatcher(w_ptr2);

    // test del
    ws.DelKeyWatcher(encode_key0, w_p0->GetWatcherId());
    ws.CheckDelKeywatcher(w_ptr0, false);

    ws.DelKeyWatcher(encode_key1, w_p1->GetWatcherId());
    ws.CheckDelKeywatcher(w_ptr1, false);

    ws.DelKeyWatcher(encode_key2, w_p2->GetWatcherId());
    ws.CheckDelKeywatcher(w_ptr2, false);
}

TEST(TestWatchServer, SimulateInteractive) {
    TestWatchServer server;

    std::vector<Key *> keys0;
    keys0.push_back(new Key("k0.1"));
    keys0.push_back(new Key("k0.2"));
    keys0.push_back(new Key("k0.10"));

    {
        auto conn0 = new TestWatchConnection(5000); // read timeout 5s

        /*
        // test timeout
    //    conn0->ServerWrite();
        ASSERT_TRUE(conn0->ClientRead() == TEST_TIMEOUT); // no server write, timeout
         */

        auto w_ptr0 = std::make_shared<TestWatcher>(keys0, conn0);
        WatcherPtr w_p0 = std::static_pointer_cast<Watcher>(w_ptr0);
        server.AddKeyWatcher(w_p0, nullptr);
        ASSERT_TRUE(conn0->ClientRead() != TEST_TIMEOUT);
    }

    {
        auto conn = new TestWatchConnection(5000, "hello", "timeout");
        auto w_ptr = std::make_shared<TestWatcher>(keys0, conn);
        WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
        server.AddKeyWatcher(w_p, nullptr);

        ASSERT_TRUE(conn->ClientRead() == TEST_TIMEOUT);
    }

    {
        auto conn = new TestWatchConnection(5000, "hello", "hello");
        auto w_ptr = std::make_shared<TestWatcher>(keys0, conn);
        WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
        server.AddKeyWatcher(w_p, nullptr);

        sleep(1);
        conn->ServerWrite(); // simulate server response

        ASSERT_TRUE(conn->ClientRead() == TEST_OK);
    }

    {
        auto conn = new TestWatchConnection(5000, "hello", "world");
        auto w_ptr = std::make_shared<TestWatcher>(keys0, conn);
        WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
        server.AddKeyWatcher(w_p, nullptr);

        sleep(1);
        conn->ServerWrite(); // simulate server response

        ASSERT_TRUE(conn->ClientRead() == TEST_UNEXPECTED);
    }

    {
        // test concurrent timeout response
        int i;
        int N = 1000;
        int TEST_TIMES = 3;
        std::thread th[N];
        for (auto j = 0; j < TEST_TIMES; j++) {
            for (i = 0; i < N; i++) {
                th[i] = std::thread([&]() {
                    auto conn = new TestWatchConnection(5000, "hello", "timeout"); // read timeout 5s
                    auto w_ptr = std::make_shared<TestWatcher>(keys0, conn);
                    WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
                    server.AddKeyWatcher(w_p, nullptr);
                    ASSERT_TRUE(conn->ClientRead() == TEST_TIMEOUT);
                });
            }
            for (i = 0; i < N; i++) {
                th[i].join();
            }
        }

        for (auto j = 0; j < TEST_TIMES; j++) {
            for (i = 0; i < N; i++) {
                th[i] = std::thread([&]() {
                    auto conn = new TestWatchConnection(5000); // read timeout 5s
                    auto w_ptr = std::make_shared<TestWatcher>(keys0, conn);
                    WatcherPtr w_p = std::static_pointer_cast<Watcher>(w_ptr);
                    server.AddKeyWatcher(w_p, nullptr);

                    sleep(1);
                    conn->ServerWrite(); // simulate server response

                    ASSERT_TRUE(conn->ClientRead() == TEST_OK);
                });
            }
            for (i = 0; i < N; i++) {
                th[i].join();
            }
        }
    }
}

} // namespace
