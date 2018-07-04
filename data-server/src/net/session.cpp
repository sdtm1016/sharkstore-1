#include "session.h"

#include <asio/read.hpp>
#include <asio/read_until.hpp>

#include "frame/sf_logger.h"

#include "msg_handler.h"

namespace sharkstore {
namespace dataserver {
namespace net {

std::atomic<uint64_t> Session::total_count_ = {0};

Session::Session(const SessionOptions& opt, const MsgHandler& msg_handler,
                 asio::ip::tcp::socket socket)
    : opt_(opt), msg_handler_(msg_handler), socket_(std::move(socket)) {
    ++total_count_;
}

Session::~Session() {
    doClose();

    --total_count_;

    FLOG_DEBUG("%s destroyed.", id_.c_str());
}

bool Session::init() {
    try {
        auto local_ep = socket_.local_endpoint();
        msg_ctx_.local_addr =
            local_ep.address().to_string() + ":" + std::to_string(local_ep.port());

        auto remote_ep = socket_.remote_endpoint();
        msg_ctx_.remote_addr =
            remote_ep.address().to_string() + ":" + std::to_string(remote_ep.port());
    } catch (std::exception& e) {
        FLOG_ERROR("[S] get socket addr error: %s", e.what());
        return false;
    }

    msg_ctx_.session = shared_from_this();
    id_ = std::string("S[") + msg_ctx_.remote_addr + "]";

    FLOG_INFO("%s establised %s->%s", id_.c_str(), msg_ctx_.remote_addr.c_str(),
              msg_ctx_.local_addr.c_str());

    return true;
}

void Session::Start() {
    if (init()) {
        readPreface();
    } else {
        doClose();
    }
}

void Session::doClose() {
    if (closed_) {
        return;
    }

    closed_ = true;
    asio::error_code ec;
    socket_.close(ec);
    (void)ec;

    FLOG_INFO("%s closed. ", id_.c_str());
}

void Session::readPreface() {
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(preface_.data(), preface_.size()),
                     [this, self](std::error_code ec, std::size_t) {
                         if (!ec) {
                             if (ValidRPCMagic(preface_)) {
                                 readRPCHead();
                             } else {
                                 parseCmdLine(0);
                                 readCmdLine();
                             }
                         } else {
                             FLOG_ERROR("%s read perface error: %s", id_.c_str(),
                                        ec.message().c_str());
                         }
                     });
}

void Session::readRPCHead() {
    void* buf = &rpc_head_;
    size_t buf_size = sizeof(rpc_head_);
    if (preface_remained_ > 0) {
        buf = static_cast<char*>(buf) + preface_.size();
        buf_size -= preface_.size();
        preface_remained_ = 0;
    }
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(buf, buf_size),
                     [this, self](std::error_code ec, std::size_t) {
                         if (!ec) {
                             rpc_head_.Decode();
                             if (rpc_head_.Valid()) {
                                 readRPCBody();
                             } else {
                                 FLOG_ERROR("%s invalid rpc head: %s", id_.c_str(),
                                            rpc_head_.DebugString().c_str());
                             }
                         } else {
                             FLOG_ERROR("%s read rpc head error: %s", id_.c_str(),
                                        ec.message().c_str());
                         }
                     });
}

void Session::readRPCBody() {
    rpc_body_.resize(rpc_head_.body_length);
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(rpc_body_.data(), rpc_body_.size()),
                     [this, self](std::error_code ec, std::size_t) {
                         if (!ec) {
                             msg_handler_.rpc_handler(msg_ctx_, rpc_head_,
                                                      std::move(rpc_body_));
                             readRPCHead();
                         } else {
                             FLOG_ERROR("%s read rpc body error: %s", id_.c_str(),
                                        ec.message().c_str());
                         }
                     });
}

void Session::parseCmdLine(std::size_t length) {
    std::string cmdline;
    // consume preface
    if (preface_remained_ > 0) {
        assert(preface_remained_ <= 4);
        uint8_t ch = static_cast<uint8_t>('\n');
        for (int i = 4 - preface_remained_; i < 4; ++i) {
            if (preface_[i] == ch) {
                preface_remained_ -= (cmdline.size() + 1);
                msg_handler_.telnet_handler(msg_ctx_, std::move(cmdline));
                cmdline.clear();
            } else {
                cmdline.push_back(static_cast<char>(preface_[i]));
            }
        }
    }
    if (length > 0) {
        preface_remained_ = 0;
        cmdline.append(asio::buffer_cast<const char*>(cmdline_buffer_.data()), length);
        msg_handler_.telnet_handler(msg_ctx_, std::move(cmdline));
        cmdline_buffer_.consume(length);
    }
}

void Session::readCmdLine() {
    auto self(shared_from_this());
    asio::async_read_until(socket_, cmdline_buffer_, '\n',
                           [this, self](std::error_code ec, std::size_t length) {
                               if (!ec) {
                                   parseCmdLine(length);
                                   readCmdLine();
                               } else {
                                   FLOG_ERROR("%s read cmdline error: %s", id_.c_str(),
                                              ec.message().c_str());
                               }
                           });
}

} /* net */
} /* dataserver  */
} /* sharkstore  */
