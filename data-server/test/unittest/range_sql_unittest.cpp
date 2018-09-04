#include <gtest/gtest.h>

#include "helper/range_test_fixture.h"
#include "helper/helper_util.h"
#include "helper/query_builder.h"
#include "helper/query_parser.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::test::helper;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;

TEST_F(RangeTestFixture, NoLeader) {
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
    }
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header());
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
    }
    {
        DsDeleteRequest req;
        MakeHeader((req.mutable_header()));
        DsDeleteResponse resp;
        auto s = TestDelete(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
    }
}

TEST_F(RangeTestFixture, NotLeader) {
    SetLeader(2);
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_EQ(resp.header().error().not_leader().leader().node_id(), 2);
        ASSERT_EQ(resp.header().error().not_leader().leader().id(), GetPeerID(2));
    }
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header());
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_EQ(resp.header().error().not_leader().leader().node_id(), 2);
        ASSERT_EQ(resp.header().error().not_leader().leader().id(), GetPeerID(2));
    }
    {
        DsDeleteRequest req;
        MakeHeader((req.mutable_header()));
        DsDeleteResponse resp;
        auto s = TestDelete(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_EQ(resp.header().error().not_leader().leader().node_id(), 2);
        ASSERT_EQ(resp.header().error().not_leader().leader().id(), GetPeerID(2));
    }
}

TEST_F(RangeTestFixture, StaleEpoch) {
    SetLeader(GetNodeID());
    auto old_ver = range_->options().range_epoch().version();
    auto s = Split();
    ASSERT_TRUE(s.ok()) << s.ToString();
    auto old_meta = range_->options();
    auto split_range = context_->FindRange(GetSplitRangeID());
    ASSERT_TRUE(split_range != nullptr);
    auto new_meta = split_range->options();
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header(), old_ver);
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());
        auto &stale_err = resp.header().error().stale_epoch();
        ASSERT_EQ(old_meta.ShortDebugString(), stale_err.old_range().ShortDebugString());
        ASSERT_EQ(new_meta.ShortDebugString(), stale_err.new_range().ShortDebugString());
    }
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header(), old_ver);
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());
        auto &stale_err = resp.header().error().stale_epoch();
        ASSERT_EQ(old_meta.ShortDebugString(), stale_err.old_range().ShortDebugString());
        ASSERT_EQ(new_meta.ShortDebugString(), stale_err.new_range().ShortDebugString());
    }
    {
        DsDeleteRequest req;
        MakeHeader(req.mutable_header(), old_ver);
        DsDeleteResponse resp;
        auto s = TestDelete(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());
        auto &stale_err = resp.header().error().stale_epoch();
        ASSERT_EQ(old_meta.ShortDebugString(), stale_err.old_range().ShortDebugString());
        ASSERT_EQ(new_meta.ShortDebugString(), stale_err.new_range().ShortDebugString());
    }
}

TEST_F(RangeTestFixture, CURD) {
    SetLeader(GetNodeID());

    std::vector<std::vector<std::string>> rows = {
            {"1", "user1", "111"},
            {"2", "user2", "222"},
            {"3", "user3", "333"},
    };

    // insert some rows
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header());
        InsertRequestBuilder builder(table_.get());
        builder.AddRows(rows);
        req.mutable_req()->CopyFrom(builder.Build());
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.resp().code(), 0);
        ASSERT_EQ(resp.resp().affected_keys(), rows.size());
    }
    // test select
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.AddAllFields();
        *req.mutable_req() = builder.Build();
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.req(), resp.resp());
        s = parser.Match(rows);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // test delete one
    {
        DsDeleteRequest req;
        MakeHeader(req.mutable_header());
        DeleteRequestBuilder builder(table_.get());
        builder.AddMatch("id", kvrpcpb::Equal, "1");
        *req.mutable_req() = builder.Build();
        DsDeleteResponse resp;
        auto s = TestDelete(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.resp().code(), 0);
        ASSERT_EQ(resp.resp().affected_keys(), 1);
    }
    // select and check
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.AddAllFields();
        *req.mutable_req() = builder.Build();
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.req(), resp.resp());
        s = parser.Match({rows[1], rows[2]});
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // delete all
    {
        DsDeleteRequest req;
        MakeHeader(req.mutable_header());
        DeleteRequestBuilder builder(table_.get());
        *req.mutable_req() = builder.Build();
        DsDeleteResponse resp;
        auto s = TestDelete(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.resp().code(), 0);
        ASSERT_EQ(resp.resp().affected_keys(), 2);
    }
    // select and check
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.AddAllFields();
        *req.mutable_req() = builder.Build();
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.req(), resp.resp());
        s = parser.Match({});
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

}

