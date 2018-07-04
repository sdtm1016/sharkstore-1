#include <gtest/gtest.h>

#include "base/util.h"
#include "raft/src/impl/raft_types.h"
#include "test_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore;
using namespace sharkstore::raft;
using namespace sharkstore::raft::impl;
using namespace sharkstore::raft::impl::testutil;

TEST(RaftTypes, PeerCoding) {
    for (int i = 0; i < 30; ++i) {
        auto peer = RandomPeer();
        pb::Peer pb_peer;
        auto s = EncodePeer(peer, &pb_peer);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(pb_peer.node_id(), peer.node_id);
        ASSERT_EQ(pb_peer.peer_id(), peer.peer_id);
        if (peer.type == PeerType::kLearner) {
            ASSERT_EQ(pb_peer.type(), pb::PEER_LEARNER);
        } else {
            ASSERT_EQ(pb_peer.type(), pb::PEER_NORMAL);
        }
        std::cout << peer.ToString() << std::endl;

        Peer peer2;
        s = DecodePeer(pb_peer, &peer2);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(EqualPeer(peer2, peer));
    }
}

TEST(RaftTypes, ConfChangeCoding) {
    for (int i = 0; i < 30; ++i) {
        ConfChange cc;
        cc.type = static_cast<ConfChangeType>(randomInt() % 3);
        cc.peer = RandomPeer();
        cc.context = randomString(100);

        std::string str;
        auto s = EncodeConfChange(cc, &str);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ConfChange cc2;
        s = DecodeConfChange(str, &cc2);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(cc2.type, cc.type);
        ASSERT_TRUE(EqualPeer(cc2.peer, cc.peer));
        ASSERT_EQ(cc2.context, cc.context);
    }
}
}
