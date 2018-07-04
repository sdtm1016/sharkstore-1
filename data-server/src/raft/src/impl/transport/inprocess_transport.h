_Pragma("once");

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

#include "base/shared_mutex.h"
#include "transport.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

class InProcessTransport : public Transport {
public:
    explicit InProcessTransport(uint64_t node_id);
    ~InProcessTransport();

    Status Start(const std::string& listen_ip, uint16_t listen_port,
                 const MessageHandler& handler) override;
    void Shutdown() override;

    void SendMessage(MessagePtr& msg) override;

    Status GetConnection(uint64_t to, std::shared_ptr<Connection>* conn) override;

private:
    void recvRoutine();

private:
    class InProcessConn : public Connection {
    public:
        explicit InProcessConn(InProcessTransport* t) : t_(t) {}
        Status Send(MessagePtr& msg) override {
            t_->SendMessage(msg);
            return Status::OK();
        }

        Status Close() override { return Status::OK(); }

    private:
        InProcessTransport* t_ = nullptr;
    };

    class MailBox {
    public:
        MailBox();
        ~MailBox();

        void send(const MessagePtr& msg);
        bool recv(MessagePtr* msg);
        void close();

    private:
        bool running_ = false;
        std::queue<MessagePtr> msgs_;
        std::mutex mu_;
        std::condition_variable cv_;
    };

    class MsgHub {
    public:
        MsgHub();
        ~MsgHub();

        std::shared_ptr<MailBox> regist(uint64_t node_id);
        void unregister(uint64_t node_id);
        void send(const MessagePtr& msg);

    private:
        std::map<uint64_t, std::shared_ptr<MailBox>> mail_boxes_;
        mutable sharkstore::shared_mutex mu_;
    };

private:
    static MsgHub msg_hub_;

private:
    const uint64_t node_id_;
    MessageHandler handler_;

    std::atomic<bool> running_ = {true};
    std::shared_ptr<MailBox> mail_box_;
    std::unique_ptr<std::thread> pull_thr_;
};

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
