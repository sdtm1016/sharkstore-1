#include "status.h"
#include <cstring>

namespace sharkstore {

const char *Status::copyState(const char *s) {
    char *const result = new char[strlen(s) + 1];
    strcpy(result, s);
    return result;
}

Status::Status(Code code, const std::string &msg1, const std::string &msg2)
    : code_(code) {
    const size_t len1 = msg1.size();
    const size_t len2 = msg2.size();
    const size_t size = len1 + (len2 > 0 ? (len2 + 2) : 0);
    char *const result = new char[size + 1];
    memcpy(result, msg1.data(), len1);
    if (len2 > 0) {
        result[len1] = ':';
        result[len1 + 1] = ' ';
        memcpy(result + len1 + 2, msg2.data(), len2);
    }
    result[size] = '\0';
    state_ = result;
}

std::string Status::ToString() const {
    char tmp[30];
    const char *type;
    switch (code_) {
        case kOk:
            return "ok";
        case kNotFound:
            type = "NotFound: ";
            break;
        case kCorruption:
            type = "Corruption: ";
            break;
        case kNotSupported:
            type = "NotSupported: ";
            break;
        case kInvalidArgument:
            type = "Invalid argument: ";
            break;
        case kIOError:
            type = "IO error: ";
            break;
        case kShutdownInProgress:
            type = "Shutdown in progress: ";
            break;
        case kTimedOut:
            type = "Timedout: ";
            break;
        case kAborted:
            type = "Aborted: ";
            break;
        case kBusy:
            type = "Busy: ";
            break;
        case kExpired:
            type = "Expired: ";
            break;
        case kDuplicate:
            type = "Duplicate: ";
            break;
        case kCompacted:
            type = "Compacted: ";
            break;
        case kEndofFile:
            type = "EOF: ";
            break;
        case kNoLeader:
            type = "No Leader: ";
            break;
        case kNotLeader:
            type = "Not Leader: ";
            break;
        case kStaleEpoch:
            type = "StaleEpoch: ";
            break;
        case kExisted:
            type = "Existed: ";
            break;
        case kNoMem:
            type = "No Memory: ";
            break;
        case kStaleRange:
            type = "Stale Range: ";
            break;
        case kInvalid:
            type = "Invalid: ";
            break;
        case kResourceExhaust:
            type = "Resource is Exhaust: ";
            break;
        case kNoLeftSpace:
            type = "No Left Space: ";
            break;
        case kUnexpected:
            type = "Unexpected: ";
            break;
        case kOutOfBound:
            type = "Out Of Bound: ";
            break;
        default:
            snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code_));
            type = tmp;
    }
    std::string result(type);
    if (state_ != nullptr) {
        result.append(state_);
    }
    return result;
}

} /* namespace sharkstore */
