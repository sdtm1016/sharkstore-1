#ifndef FBASE_DATASERVER_SERVER_RAFT_LOGGER_H_
#define FBASE_DATASERVER_SERVER_RAFT_LOGGER_H_

#include "raft/logger.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class RaftLogger : public raft::Logger {
public:
    RaftLogger() = default;
    ~RaftLogger() = default;

    bool IsEnableDebug() override;
    bool IsEnableInfo() override;
    bool IsEnableWarn() override;

    void Debug(const char* file, int line, const char* format, ...) override;
    void Info(const char* file, int line, const char* format, ...) override;
    void Warn(const char* file, int line, const char* format, ...) override;
    void Error(const char* file, int line, const char* format, ...) override;
};

} /* namespace server */
} /* namespace dataserver */
} /* namespace sharkstore */

#endif /* end of include guard: FBASE_DATASERVER_SERVER_RAFT_LOGGER_H_ */