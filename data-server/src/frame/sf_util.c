#include "sf_util.h"

#include <stdarg.h>
#include <stdio.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/time.h>
#include <pthread.h>

int64_t getticks() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + (int64_t)tv.tv_usec / 1000;
}

int64_t get_micro_second() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000000 + (int64_t)tv.tv_usec;
}


int64_t get_second() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec;
}

void set_thread_name(pthread_t handle, const char *name) {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
    pthread_setname_np(handle, name);
#endif
#endif

}
