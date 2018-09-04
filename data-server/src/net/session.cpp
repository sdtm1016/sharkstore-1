#include "session.h"

#include <asio/read.hpp>
#include <asio/write.hpp>
#include <asio/read_until.hpp>

#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace net {

std::atomic<uint64_t> Session::total_count_ = {0};

Session::Session(const SessionOptions& opt, const Handler & handler,
                 asio::ip::tcp::socket socket)
    : opt_(opt), handler_(handler), socket_(std::move(socket)) {
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
        session_ctx_.local_addr =
            local_ep.address().to_string() + ":" + std::to_string(local_ep.port());

        auto remote_ep = socket_.remote_endpoint();
        session_ctx_.remote_addr =
            remote_ep.address().to_string() + ":" + std::to_string(remote_ep.port());
    } catch (std::exception& e) {
        FLOG_ERROR("[S] get socket addr error: %s", e.what());
        return false;
    }

    session_ctx_.session = shared_from_this();
    id_ = std::string("S[") + session_ctx_.remote_addr + "]";

    FLOG_INFO("%s establised %s->%s", id_.c_str(), session_ctx_.remote_addr.c_str(),
              session_ctx_.local_addr.c_str());

    return true;
}

void Session::Start() {
    if (init()) {
        readHead();
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

    session_ctx_.session.reset();

    FLOG_INFO("%s closed. ", id_.c_str());
}

void Session::readHead() {
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(&head_, sizeof(head_)),
                     [this, self](std::error_code ec, std::size_t) {
                         if (!ec) {
                             head_.Decode();
                             auto ret = head_.Valid();
                             if (ret.ok()) {
                                 readBody();
                             } else {
                                 FLOG_ERROR("%s invalid rpc head: %s", id_.c_str(), ret.ToString().c_str());
                                 doClose();
                             }
                         } else {
                             if (ec == asio::error::eof) {
                                 FLOG_INFO("%s read rpc head error: %s", id_.c_str(),
                                            ec.message().c_str());
                             } else {
                                 FLOG_ERROR("%s read rpc head error: %s", id_.c_str(),
                                            ec.message().c_str());
                             }
                             doClose();
                         }
                     });
}

void Session::readBody() {
    if (head_.body_length == 0) {
        if (head_.func_id == kHeartbeatFuncID) { // response heartbeat
            auto msg = NewMessage();
            msg->head.SetResp(head_);
            Write(msg);
        }
        readHead();
        return;
    }

    body_.resize(head_.body_length);
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(body_.data(), body_.size()),
                     [this, self](std::error_code ec, std::size_t) {
                         if (!ec) {
                             auto msg = NewMessage();
                             msg->head = head_;
                             msg->body = std::move(body_);
                             handler_(session_ctx_, msg);

                             readHead();
                         } else {
                             if (ec == asio::error::eof) {
                                 FLOG_INFO("%s read rpc body error: %s", id_.c_str(),
                                            ec.message().c_str());
                             } else {
                                 FLOG_ERROR("%s read rpc body error: %s", id_.c_str(),
                                            ec.message().c_str());
                             }
                             doClose();
                         }
                     });
}

void Session::doWrite() {
    auto self(shared_from_this());

    // prepare write buffer
    auto msg = write_msgs_.front();
    msg->head.body_length = static_cast<uint32_t>(msg->body.size());
    msg->head.Encode();
    std::vector<asio::const_buffer> buffers{
        asio::buffer(&msg->head, sizeof(msg->head)),
        asio::buffer(msg->body.data(), msg->body.size())
    };

    asio::async_write(socket_, buffers,
                      [this, self](std::error_code ec, std::size_t /*length*/) {
                          if (!ec) {
                              write_msgs_.pop_front();
                              if (!write_msgs_.empty()) {
                                  doWrite();
                              }
                          } else {
                              FLOG_ERROR("%s write message error: %s", id_.c_str(), ec.message().c_str());
                              doClose();
                          }
                      });
}

void Session::Write(const MessagePtr& msg) {
    auto self(shared_from_this());
    asio::post(socket_.get_io_context(), [self, msg] {
        bool write_in_progress = !self->write_msgs_.empty();
        self->write_msgs_.push_back(msg);
        if (!write_in_progress) {
            self->doWrite();
        }
    });
}

} /* net */
} /* dataserver  */
} /* sharkstore  */
