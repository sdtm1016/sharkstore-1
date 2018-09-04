#include "sf_logger.h"

#include <string.h>

const char* get_log_level_name(int level) {
    switch (level) {
        case LOG_DEBUG:
            return "debug";
        case LOG_INFO:
            return "info";
        case LOG_WARNING:
            return "warn";
        case LOG_ERR:
            return "error";
        case LOG_CRIT:
            return "crit";
        default:
            return "unknown";
    }
}

int get_log_level_from_name(const char *name) {
    if (strcmp(name, "DEBUG") == 0 || strcmp(name, "debug") == 0) {
        return LOG_DEBUG;
    } else if (strcmp(name, "INFO") == 0 || strcmp(name, "info") == 0) {
        return LOG_INFO;
    } else if (strcmp(name, "ERROR") == 0 || strcmp(name, "error") == 0) {
        return LOG_ERR;
    } else if (strcmp(name, "WARN") == 0 || strcmp(name, "warn") == 0 ||
        strcmp(name, "WARNING") == 0 || strcmp(name, "warning") == 0) {
        return LOG_WARNING;
    } else if (strcmp(name, "CRIT") == 0 || strcmp(name, "crit") == 0) {
        return LOG_CRIT;
    } else {
        return LOG_INFO;
    }
}

