#include "connection.h"

namespace sharkstore {
namespace dataserver {
namespace master {

Connection::Connection(const std::string &addr, grpc::CompletionQueue &cq)
    : addr_(addr),
      cq_(cq),
      channel_(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())),
      stub_(new mspb::MsServer::Stub(channel_)) {
    assert(!addr_.empty());
}

Connection::~Connection() {}

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
