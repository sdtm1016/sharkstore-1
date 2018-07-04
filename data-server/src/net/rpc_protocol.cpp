#include "rpc_protocol.h"

#include <iomanip>
#include <sstream>
#include "base/byte_order.h"

namespace sharkstore {
namespace dataserver {
namespace net {

bool RPCHead::Valid() const {
    bool valid_magic = std::equal(magic, magic + 4, kRPCMagicV1.cbegin()) ||
                       std::equal(magic, magic + 4, kRPCMagicV2.cbegin());
    if (!valid_magic) {
        return false;
    }
    if (msg_type != kRPCRequestType && msg_type != kRPCResponseType) {
        return false;
    }
    if (body_length > kMaxRPCBodyLength) {
        return false;
    }
    return true;
}

void RPCHead::Encode() {
    version = htobe16(version);
    msg_type = htobe16(msg_type);
    func_id = htobe16(func_id);
    msg_id = htobe64(msg_id);
    timeout = htobe32(timeout);
    body_length = htobe32(body_length);
}

void RPCHead::Decode() {
    version = be16toh(version);
    msg_type = be16toh(msg_type);
    func_id = be16toh(func_id);
    msg_id = be64toh(msg_id);
    timeout = be32toh(timeout);
    body_length = be32toh(body_length);
}

std::string RPCHead::DebugString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"magic\": \"0x";
    for (auto c : magic) {
        ss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(c);
    }
    ss << "\", ";
    ss << "\"vesrion\": " << version << ", ";
    ss << "\"msg_type\": " << msg_type << ", ";
    ss << "\"func_id\": " << func_id << ", ";
    ss << "\"msg_id\": " << msg_id << ", ";
    ss << "\"body_len\":" << body_length;
    ss << "}";

    return ss.str();
}

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
