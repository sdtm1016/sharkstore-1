#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>

#include <fastcommon/logger.h>
#include <fastcommon/process_ctrl.h>
#include <fastcommon/pthread_func.h>
#include <fastcommon/sched_thread.h>
#include <fastcommon/shared_func.h>

#include "common/ds_config.h"
#include "common/ds_proto.h"
#include "common/ds_types.h"
#include "common/ds_version.h"
#include "frame/sf_service.h"

#include "callback.h"
#include "version.h"

using sharkstore::dataserver::server::GetVersionInfo;

int main(int argc, char *argv[]) {
    // print version
    if (argc > 1 && (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0)) {
        std::cout << GetVersionInfo() << std::endl;
        return 0;
    }

    sf_set_proto_header_size(sizeof(ds_proto_header_t));
    sf_regist_body_length_callback(ds_get_body_length);
    sf_regist_load_config_callback(load_from_conf_file);

    sf_regist_print_version_callback(print_version);

    sf_regist_user_init_callback(ds_user_init_callback);
    sf_regist_user_destroy_callback(ds_user_destroy_callback);

    sf_service_run(argc, argv, "data_server");

    return 0;
}
