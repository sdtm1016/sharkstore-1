#ifndef _DS_PROTO_H_
#define _DS_PROTO_H_

#include <stdint.h>

#include <fastcommon/fast_task_queue.h>

#define DS_PROTO_VERSION_NONE 10
#define DS_PROTO_VERSION_CURRENT 17

#define DS_PROTO_MAGIC_NUMBER 0x23232323

#define DS_PROTO_FID_RPC_REQ 0x02
#define DS_PROTO_FID_RPC_RESP 0x12

enum ds_proto_flags {
    FAST_WORKER_FLAG = 1 << 0,
};

typedef struct ds_proto_header_s {
    char magic_number[4];
    char version[2];
    char msg_type[2];
    char func_id[2];
    char msg_id[8];
    char flags;
    char proto_type;
    char time_out[4];
    char body_len[4];
} ds_proto_header_t;

typedef struct ds_header_s {
    int magic_number;
    int body_len;
    int time_out;
    int64_t msg_id;
    short version;
    short msg_type;
    short func_id;
    char proto_type;
    char flags;
} ds_header_t;

#ifdef __cplusplus
extern "C" {
#endif

extern const int header_size;

void ds_proto_init();

void ds_serialize_header(const ds_header_t *header, ds_proto_header_t *proto_header);

void ds_unserialize_header(const ds_proto_header_t *proto_header, ds_header_t *header);

int ds_get_body_length(struct fast_task_info *task);

#ifdef __cplusplus
}
#endif

#endif
