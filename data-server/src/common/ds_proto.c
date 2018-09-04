#include "ds_proto.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <fastcommon/shared_func.h>
#include <fastcommon/sockopt.h>

#include "ds_define.h"
#include "frame/sf_logger.h"

const int header_size = sizeof(ds_proto_header_t);

void ds_serialize_header(const ds_header_t *header, ds_proto_header_t *proto_header) {
    int2buff(header->magic_number, proto_header->magic_number);
    int2buff(header->time_out, proto_header->time_out);
    int2buff(header->body_len, proto_header->body_len);

    long2buff(header->msg_id, proto_header->msg_id);

    short2buff(header->version, proto_header->version);
    short2buff(header->msg_type, proto_header->msg_type);
    short2buff(header->func_id, proto_header->func_id);

    proto_header->proto_type = header->proto_type;
    proto_header->flags = header->flags;
}

void ds_unserialize_header(const ds_proto_header_t *proto_header, ds_header_t *header) {
    header->magic_number = buff2int(proto_header->magic_number);
    header->time_out = buff2int(proto_header->time_out);
    header->body_len = buff2int(proto_header->body_len);

    header->msg_id = buff2long(proto_header->msg_id);

    header->version = buff2short(proto_header->version);
    header->msg_type = buff2short(proto_header->msg_type);
    header->func_id = buff2short(proto_header->func_id);

    header->proto_type = proto_header->proto_type;
    header->flags = proto_header->flags;
}

int ds_get_body_length(struct fast_task_info *task) {
    int magic_number;
    magic_number = buff2int(((ds_proto_header_t *)task->data)->magic_number);
    if (magic_number != DS_PROTO_MAGIC_NUMBER) {
        FLOG_WARN("client ip: %s, magic number: %08X != %08X", task->client_ip,
                  magic_number, DS_PROTO_MAGIC_NUMBER);

        return EINVAL;
    }
    task->length = buff2int(((ds_proto_header_t *)task->data)->body_len);
    return 0;
}
