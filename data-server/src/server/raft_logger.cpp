#include "raft_logger.h"

#include <stdarg.h>
#include <stdio.h>

#include <fastcommon/logger.h>

namespace sharkstore {
namespace dataserver {
namespace server {

static thread_local char log_buffer[2048];

bool RaftLogger::IsEnableDebug() {
    return g_log_context.log_level >= LOG_DEBUG;
}

bool RaftLogger::IsEnableInfo() { return g_log_context.log_level >= LOG_INFO; }

bool RaftLogger::IsEnableWarn() {
    return g_log_context.log_level >= LOG_WARNING;
}

void RaftLogger::Debug(const char* file, int line, const char* format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(log_buffer, 2048, format, args);
    va_end(args);
    logDebug("%s:%d %s", file, line, log_buffer);
}

void RaftLogger::Info(const char* file, int line, const char* format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(log_buffer, 2048, format, args);
    va_end(args);
    logInfo("%s:%d %s", file, line, log_buffer);
}

void RaftLogger::Warn(const char* file, int line, const char* format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(log_buffer, 2048, format, args);
    va_end(args);
    logWarning("%s:%d %s", file, line, log_buffer);
}

void RaftLogger::Error(const char* file, int line, const char* format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(log_buffer, 2048, format, args);
    va_end(args);
    logError("%s:%d %s", file, line, log_buffer);
}

} /* namespace server */
} /* namespace dataserver */
} /* namespace sharkstore */