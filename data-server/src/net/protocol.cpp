#include "protocol.h"

#include <iomanip>
#include <sstream>
#include "base/byte_order.h"

namespace sharkstore {
namespace dataserver {
namespace net {

void Head::SetResp(const Head& req) {
    func_id = req.func_id;
    msg_id = req.msg_id;
    proto_type = req.proto_type;
    if (req.msg_type == kAdminRequestType) {
        msg_type = kAdminResponseType;
    } else if (req.msg_type == kDataRequestType) {
        msg_type = kDataResponseType;
    }
}

Status Head::Valid() const {
    if (magic != kMagic) {
        return Status(Status::kInvalidArgument, "magic", std::to_string(magic));
    }
    if (msg_type != kDataRequestType && msg_type != kDataResponseType &&
        msg_type != kAdminRequestType && msg_type != kAdminResponseType) {
        return Status(Status::kInvalidArgument, "msg type", std::to_string(msg_type));
    }
    if (body_length > kMaxBodyLength) {
        return Status(Status::kInvalidArgument, "body length", std::to_string(body_length));
    }
    return Status::OK();
}

void Head::Encode() {
    magic = htobe32(magic);
    version = htobe16(version);
    msg_type = htobe16(msg_type);
    func_id = htobe16(func_id);
    msg_id = htobe64(msg_id);
    timeout = htobe32(timeout);
    body_length = htobe32(body_length);
}

void Head::Decode() {
    magic = be32toh(magic);
    version = be16toh(version);
    msg_type = be16toh(msg_type);
    func_id = be16toh(func_id);
    msg_id = be64toh(msg_id);
    timeout = be32toh(timeout);
    body_length = be32toh(body_length);
}

std::string Head::DebugString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"magic\": " << magic << ", ";
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
