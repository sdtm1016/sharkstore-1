_Pragma("once");

#include <array>
#include "base/status.h"

namespace sharkstore {
namespace dataserver {
namespace net {

static const uint32_t kMagic = 0x23232323;

static const uint16_t kCurrentVersion = 1;

static const uint16_t kAdminRequestType = 0x01;
static const uint16_t kAdminResponseType = 0x11;
static const uint16_t kDataRequestType = 0x02;
static const uint16_t kDataResponseType = 0x12;

static const uint32_t kMaxBodyLength = 20 * 1024 * 1024;  // 20Mb

static const uint16_t kHeartbeatFuncID = 0;

struct Head {
    uint32_t magic = kMagic;
    uint16_t version = kCurrentVersion;
    uint16_t msg_type = 0;
    uint16_t func_id = 0;
    uint64_t msg_id = 0;
    uint8_t stream_hash = 0;
    uint8_t proto_type = 0;
    uint32_t timeout = 0;
    uint32_t body_length = 0;

    // set from a request head, self is a response head
    void SetResp(const Head& req);

    // encode to network byte order
    void Encode();
    // decode network byte order to host order
    void Decode();

    Status Valid() const;
    std::string DebugString() const;

} __attribute__((packed));

static constexpr int kHeadSize = sizeof(Head);

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
