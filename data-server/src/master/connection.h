_Pragma("once");

#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "proto/gen/mspb.grpc.pb.h"

namespace sharkstore {
namespace dataserver {
namespace master {

class Connection {
public:
    Connection(const std::string &addr, grpc::CompletionQueue &cq);
    ~Connection();

    Connection(const Connection &) = delete;
    Connection &operator=(const Connection &) = delete;

    std::unique_ptr<mspb::MsServer::Stub> &GetStub() { return stub_; }
    const std::string &GetAddr() const { return addr_; }

private:
    const std::string addr_;
    grpc::CompletionQueue &cq_;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<mspb::MsServer::Stub> stub_;
};

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
