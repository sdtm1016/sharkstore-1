#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "base/util.h"
#include "common/ds_proto.h"

#include "raft/src/impl/transport/fast_transport.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/* namespace { */

/* using namespace sharkstore; */
/* using namespace sharkstore::raft; */
/* using namespace sharkstore::raft::impl; */

/* static const uint64_t kTestDstNodeID = 9; */
/* static const uint16_t kTestDstPort = 9999; */
/* static const uint64_t kSnapUUID = 123456; */

/* struct TestResolver : public NodeResolver { */
/* public: */
/*     std::string GetNodeAddress(uint64_t node_id) override { */
/*         if (node_id != kTestDstNodeID) { */
/*             throw std::logic_error("unkown node id"); */
/*         } */
/*         return std::string("127.0.0.1:") + std::to_string(kTestDstPort); */
/*     } */
/* }; */

/* struct TestSnapshot : public Snapshot { */
/* public: */
/*     TestSnapshot(uint64_t applied, size_t num, const std::string& context) */
/*         : applied_(applied), num_(num), context_(context) {} */

/*     Status Next(std::string* data, bool* over) override { */
/*         if (count_ > num_) { */
/*             *over = true; */
/*         } else { */
/*             *over = false; */
/*             *data = std::to_string(count_++); */
/*         } */
/*         // *over = count_ > num_; */
/*         return Status::OK(); */
/*     }; */

/*     Status Context(std::string* context) override { */
/*         *context = context_; */
/*         return Status::OK(); */
/*     } */

/*     uint64_t ApplyIndex() override { return applied_; } */
/*     void Close() override {} */

/* private: */
/*     const uint64_t applied_ = 0; */
/*     const size_t num_ = 0; */
/*     const std::string context_; */
/*     size_t count_ = 1; */
/* }; */

/* void recv_msg(int sockfd, MessagePtr& msg) { */
/*     ds_proto_header_t proto_header; */
/*     int ret = ::recv(sockfd, &proto_header, sizeof(proto_header), 0); */
/*     ASSERT_EQ(ret, sizeof(proto_header)) << "recv error: " << strErrno(errno); */
/*     ds_header_t header; */
/*     ds_unserialize_header(&proto_header, &header); */

/*     ASSERT_EQ(header.func_id, 100); */

/*     std::vector<char> buf(header.body_len, '0'); */
/*     ret = ::recv(sockfd, buf.data(), buf.size(), 0); */
/*     ASSERT_EQ(ret, buf.size()) << "recv error: " << strErrno(errno); */
/*     ASSERT_TRUE(msg->ParseFromArray(buf.data(), buf.size())); */
/* } */

/* std::mutex g_mu; */
/* std::condition_variable g_cond; */
/* bool recv_finsh = false; */
/* size_t blocks_count = 0; */

/* void recv_routine(int sockfd) { */
/*     struct sockaddr_in cli_addr; */
/*     memset(&cli_addr, 0, sizeof(cli_addr)); */
/*     socklen_t addr_len = sizeof(cli_addr); */
/*     int cli_fd = ::accept(sockfd, (sockaddr*)&cli_addr, &addr_len); */
/*     ASSERT_GT(cli_fd, 0) << "accept error: " << strErrno(errno); */

/*     uint64_t prev_seq = 0; */
/*     size_t prev_count = 0; */

/*     while (true) { */
/*         MessagePtr msg(new pb::Message); */
/*         recv_msg(cli_fd, msg); */
/*         ASSERT_EQ(msg->type(), pb::SNAPSHOT_REQUEST); */
/*         const auto& snapshot = msg->snapshot(); */
/*         ASSERT_EQ(snapshot.uuid(), kSnapUUID) << "invalid uuid: " << snapshot.uuid();
 */
/*         static bool first = true; */
/*         if (first) { */
/*             ASSERT_EQ(snapshot.meta().context(), "test context") */
/*                 << "invalid context: " << snapshot.meta().context(); */
/*             ASSERT_EQ(snapshot.seq(), 0); */
/*             prev_seq = 0; */
/*             ASSERT_FALSE(snapshot.final()); */
/*             first = false; */
/*         } else { */
/*             ASSERT_EQ(snapshot.seq(), prev_seq + 1); */
/*             prev_seq = snapshot.seq(); */
/*             for (int i = 0; i < snapshot.datas_size(); ++i) { */
/*                 size_t count = atoi(snapshot.datas(i).c_str()); */
/*                 ASSERT_EQ(count, prev_count + 1); */
/*                 prev_count = count; */
/*             } */
/*         } */

/*         if (snapshot.final()) { */
/*             ASSERT_EQ(prev_count, 456); */
/*             break; */
/*         } */
/*     }; */

/*     std::lock_guard<std::mutex> lock(g_mu); */
/*     recv_finsh = true; */
/*     g_cond.notify_one(); */
/* } */

/* void start_recv() { */
/*     int sockfd = ::socket(AF_INET, SOCK_STREAM, 0); */
/*     ASSERT_GT(sockfd, 0) << "new socket failed: " << strErrno(errno); */
/*     // bind */
/*     struct sockaddr_in addr; */
/*     memset(&addr, 0, sizeof(addr)); */
/*     addr.sin_family = AF_INET; */
/*     addr.sin_port = htons(kTestDstPort); */
/*     addr.sin_addr.s_addr = INADDR_ANY; */
/*     ASSERT_EQ(::bind(sockfd, (sockaddr*)(&addr), sizeof(addr)), 0) */
/*         << "bind socket failed: " << strErrno(errno); */
/*     // listen */
/*     ASSERT_EQ(::listen(sockfd, 1024), 0) << "listen socket failed: " <<
 * strErrno(errno); */
/*     std::thread t(std::bind(&recv_routine, sockfd)); */
/*     t.detach(); */
/*     sleep(1); */
/* }; */

/* void report_result(const MessagePtr&, const SnapshotStatus& s) { */
/*     ASSERT_TRUE(s.s.ok()) << "report send failed: " << s.s.ToString(); */
/* } */

/* void handle_msg(MessagePtr&) {} */

/* void send_snap() { */
/*     transport::Transport* transport = */
/*         new transport::FastTransport(std::make_shared<TestResolver>(), 1, 1); */
/*     auto s = transport->Start("0.0.0.0", 9998, handle_msg); */
/*     ASSERT_TRUE(s.ok()) << s.ToString(); */

/*     SnapshotSender sender(transport, 1, 10); */
/*     s = sender.Start(); */
/*     ASSERT_TRUE(s.ok()) << "start snapshot sender failed: " << s.ToString(); */

/*     auto snap_req = std::make_shared<SnapshotRequest>(); */
/*     snap_req->header = std::make_shared<pb::Message>(); */
/*     snap_req->header->set_type(pb::SNAPSHOT_REQUEST); */
/*     snap_req->header->set_to(kTestDstNodeID); */
/*     snap_req->header->mutable_snapshot()->set_uuid(kSnapUUID); */
/*     snap_req->header->mutable_snapshot()->mutable_meta()->set_context("test context");
 */
/*     snap_req->snapshot = std::make_shared<TestSnapshot>(123, 456, "test context"); */
/*     snap_req->reporter = report_result; */

/*     sender.Send(snap_req); */
/*     std::unique_lock<std::mutex> lock(g_mu); */
/*     while (!recv_finsh) { */
/*         g_cond.wait(lock); */
/*     } */
/*     sender.ShutDown(); */
/* } */

/* TEST(SnapshotSend, SendAndCheck) {} */

/* }  // namespace */
