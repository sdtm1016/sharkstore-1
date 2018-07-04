_Pragma("once");

#include <array>

namespace sharkstore {
namespace dataserver {
namespace net {

// TODO: remove v1
static const std::array<uint8_t, 4> kRPCMagicV1 = {0x23, 0x23, 0x23, 0x23};
// echo sharkstore | sha1sum | cut -c1-8 | awk '{print toupper($0)}'
static const std::array<uint8_t, 4> kRPCMagicV2 = {0xA1, 0xEA, 0xD4, 0xC6};

static const uint16_t kCurrentRPCVersion = 1;

static const uint16_t kRPCRequestType = 0x02;
static const uint16_t kRPCResponseType = 0x12;

static const uint32_t kMaxRPCBodyLength = 20 * 1024 * 1024;  // 20Mb

struct RPCHead {
    uint8_t magic[4] = {kRPCMagicV1[0], kRPCMagicV1[1], kRPCMagicV1[2], kRPCMagicV1[3]};
    uint16_t version = kCurrentRPCVersion;
    uint16_t msg_type = 0;
    uint16_t func_id = 0;
    uint64_t msg_id = 0;
    uint8_t stream_hash = 0;
    uint8_t proto_type = 0;
    uint32_t timeout = 0;
    uint32_t body_length = 0;

    // encode to network byte order
    void Encode();
    // decode network byte order to host order
    void Decode();

    bool Valid() const;
    std::string DebugString() const;

} __attribute__((packed));

static constexpr int kRPCHeadSize = sizeof(RPCHead);

inline bool ValidRPCMagic(const std::array<uint8_t, 4>& magic) {
    return magic == kRPCMagicV1 || magic == kRPCMagicV2;
}

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
