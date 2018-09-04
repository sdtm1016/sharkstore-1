_Pragma("once");

#include <fastcommon/logger.h>
#include <stdarg.h>

#define RANGE_LOG_DEBUG(fmt, ...) \
    do {\
        if (g_log_context.log_level >= LOG_DEBUG) {\
            logDebug("%s:%d range[%" PRIu64 "] " fmt, __FNAME__, __LINE__, id_, ##__VA_ARGS__);\
        }\
    } while (false)

#define RANGE_LOG_INFO(fmt, ...) \
    do  {\
        if (g_log_context.log_level >= LOG_INFO) { \
            logInfo("%s:%d range[%" PRIu64 "] " fmt, __FNAME__, __LINE__, id_, ##__VA_ARGS__);\
        }\
    } while (false)

#define RANGE_LOG_WARN(fmt, ...) \
    do  {\
        if (g_log_context.log_level >= LOG_WARNING) { \
            logWarning("%s:%d range[%" PRIu64 "] " fmt, __FNAME__, __LINE__, id_, ##__VA_ARGS__);\
        }\
    } while (false)

#define RANGE_LOG_ERROR(fmt, ...) \
    do  {\
        if (g_log_context.log_level >= LOG_ERR) { \
            logError("%s:%d range[%" PRIu64 "] " fmt, __FNAME__, __LINE__, id_, ##__VA_ARGS__);\
        }\
    } while (false)

#define RANGE_LOG_CRIT(fmt, ...) \
    do  {\
        if (g_log_context.log_level >= LOG_CRIT) { \
            logCrit("%s:%d range[%" PRIu64 "] " fmt, __FNAME__, __LINE__, id_, ##__VA_ARGS__);\
        }\
    } while (false)
