#include "logger.h"

#include <cstdarg>
#include <iostream>

namespace sharkstore {
namespace raft {
namespace impl {

Logger* g_logger = new StdLogger;

static thread_local char log_buffer[2048];

StdLogger::StdLogger(Level lvl) : lvl_(lvl) {}
bool StdLogger::IsEnableDebug() { return lvl_ <= Level::kDebug; }
bool StdLogger::IsEnableInfo() { return lvl_ <= Level::kInfo; }
bool StdLogger::IsEnableWarn() { return true; }

void StdLogger::Debug(const char* file, int line, const char* format, ...) {
    if (!IsEnableDebug()) return;
    va_list args;
    va_start(args, format);
    vsnprintf(log_buffer, 2048, format, args);
    va_end(args);
    std::cout << file << ":" << line << ": " << log_buffer << std::endl;
}

void StdLogger::Info(const char* file, int line, const char* format, ...) {
    if (!IsEnableInfo()) return;
    va_list args;
    va_start(args, format);
    vsnprintf(log_buffer, 2048, format, args);
    va_end(args);
    std::cout << file << ":" << line << ": " << log_buffer << std::endl;
}

void StdLogger::Warn(const char* file, int line, const char* format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(log_buffer, 2048, format, args);
    va_end(args);
    std::cout << file << ":" << line << ": " << log_buffer << std::endl;
}

void StdLogger::Error(const char* file, int line, const char* format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(log_buffer, 2048, format, args);
    va_end(args);
    std::cout << file << ":" << line << ": " << log_buffer << std::endl;
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
