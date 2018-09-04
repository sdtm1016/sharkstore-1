#ifndef __SF_SERVICE_H__
#define __SF_SERVICE_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fastcommon/fast_task_queue.h>
#include <fastcommon/ioevent.h>

#include "sf_config.h"
#include "sf_socket.h"
#include "sf_socket_buff.h"
#include "sf_socket_session.h"

typedef int (*sf_user_init_callback_t)();
typedef void (*sf_user_destroy_callback_t)();
typedef void (*sf_print_version_callback_t)();

#ifdef __cplusplus
extern "C" {
#endif

void sf_regist_print_version_callback(sf_print_version_callback_t print_version_func);

void sf_regist_user_init_callback(sf_user_init_callback_t user_init_func);

void sf_regist_user_destroy_callback(sf_user_destroy_callback_t user_destroy_func);

static inline void sf_regist_load_config_callback(
    sf_load_config_callback_t load_config_func) {
    sf_set_load_config_callback(load_config_func);
}

static inline void sf_regist_body_length_callback(
    sf_body_length_callback_t body_length_func) {
    sf_set_body_length_callback(body_length_func);
}

static inline void sf_set_proto_header_size(int size) { sf_set_header_size(size); }

int sf_service_run(int argc, char *argv[], const char *server_name);
int sf_service_run_test(const char* confStr);
#ifdef __cplusplus
}
#endif

#endif  //__SF_SERVICE_H__
