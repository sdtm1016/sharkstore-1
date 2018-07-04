_Pragma("once");

#include <string.h>
#include <string>
#include "raft/logger.h"

namespace sharkstore {
namespace raft {
namespace impl {

class StdLogger : public Logger {
public:
    enum class Level : char {
        kDebug = 0,
        kInfo = 1,
        kWarn = 2,
    };

    explicit StdLogger(Level lvl = Level::kDebug);

    bool IsEnableDebug() override;
    bool IsEnableInfo() override;
    bool IsEnableWarn() override;

    void Debug(const char* file, int line, const char* format, ...) override;
    void Info(const char* file, int line, const char* format, ...) override;
    void Warn(const char* file, int line, const char* format, ...) override;
    void Error(const char* file, int line, const char* format, ...) override;

private:
    Level lvl_;
};

extern Logger* g_logger;

#define LOG_DEBUG(x...) \
    if (g_logger->IsEnableDebug()) g_logger->Debug(__FNAME__, __LINE__, ##x)

#define LOG_INFO(x...) \
    if (g_logger->IsEnableInfo()) g_logger->Info(__FNAME__, __LINE__, ##x)

#define LOG_WARN(x...) \
    if (g_logger->IsEnableWarn()) g_logger->Warn(__FNAME__, __LINE__, ##x)

#define LOG_ERROR(x...) g_logger->Error(__FNAME__, __LINE__, ##x)

}  // namespace impl
}  // namespace raft
}  // namespace sharkstore
