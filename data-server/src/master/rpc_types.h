_Pragma("once");

#include <grpc++/grpc++.h>

#include "proto/gen/mspb.grpc.pb.h"

namespace sharkstore {
namespace dataserver {
namespace master {

static const int kRpcTimeoutMs = 5000;

enum class AsyncCallType : char {
    kInvalid = 0,
    kAskSplit,
    kReportSplit,
    kNodeHeartbeat,
    kRangeHeartbeat,
    kGetMSLeader,
};

std::string AsyncCallTypeName(AsyncCallType type);

struct AsyncCallResult {
    grpc::ClientContext context;
    grpc::Status status;
    AsyncCallType type = AsyncCallType::kInvalid;

    AsyncCallResult();
    virtual ~AsyncCallResult() {}
};

template <class ResponseType>
struct AsyncCallResultT : public AsyncCallResult {
public:
    ResponseType response;
    std::unique_ptr<grpc::ClientAsyncResponseReader<ResponseType>>
        response_reader;

    void Finish() { response_reader->Finish(&response, &status, (void*)this); }
};

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
