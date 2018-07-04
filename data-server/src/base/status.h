_Pragma("once");

#include <string>
#include <utility>

namespace sharkstore {

class Status {
public:
    enum Code {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5,
        kShutdownInProgress = 6,
        kTimedOut = 7,
        kAborted = 8,
        kBusy = 9,
        kExpired = 10,
        kDuplicate = 11,
        kCompacted = 12,
        kEndofFile = 13,

        kNoLeader = 14,
        kNotLeader = 15,
        kStaleEpoch = 16,
        kExisted = 17,
        kNoMem = 18,
        kStaleRange = 19,
        kInvalid = 20,
        kResourceExhaust = 21,
        kNoLeftSpace = 22,

        kUnknown = 255,
    };

    Status() : code_(kOk), state_(nullptr) {}
    explicit Status(Code code) : code_(code), state_(nullptr) {}
    Status(Code code, const std::string& msg1, const std::string& msg2);
    ~Status() { delete[] state_; }

    Status(const Status& s);
    Status& operator=(const Status& s);

    Status(Status&& s) noexcept;
    Status& operator=(Status&& s) noexcept;

    bool operator==(const Status& s) const;
    bool operator!=(const Status& s) const;

    Code code() const { return code_; }

    static Status OK() { return Status(); }

    bool ok() const { return code_ == kOk; }

    std::string ToString() const;

private:
    static const char* copyState(const char* s);

private:
    Code code_;
    const char* state_;
};

inline Status::Status(const Status& s) : code_(s.code_) {
    state_ = (s.state_ == nullptr) ? nullptr : copyState(s.state_);
}

inline Status& Status::operator=(const Status& s) {
    if (this != &s) {
        code_ = s.code_;
        delete[] state_;
        state_ = (s.state_ == nullptr) ? nullptr : copyState(s.state_);
    }
    return *this;
}

inline Status::Status(Status&& s) noexcept : Status() { *this = std::move(s); }

inline Status& Status::operator=(Status&& s) noexcept {
    if (this != &s) {
        code_ = std::move(s.code_);
        s.code_ = kOk;
        delete[] state_;
        state_ = nullptr;
        std::swap(state_, s.state_);
    }
    return *this;
}

inline bool Status::operator==(const Status& s) const { return code_ == s.code_; }
inline bool Status::operator!=(const Status& s) const { return !(*this == s); }

} /* namespace sharkstore */
