#ifndef FBASE_DATAERVER_SERVER_DS_CALLBACK_H_
#define FBASE_DATAERVER_SERVER_DS_CALLBACK_H_

#include <vector>

#include "common/ds_proto.h"
#include "frame/sf_socket_buff.h"

#ifdef __cplusplus
extern "C" {
#endif

void ds_worker_deal_callback(request_buff_t *request, void *args);

void ds_send_done_callback(response_buff_t *response, void *args, int err);

int ds_user_init_callback();
void ds_user_destroy_callback();

#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif /* end of include guard: FBASE_DATAERVER_SERVER_DS_CALLBACK_H_ */
