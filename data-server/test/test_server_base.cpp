//
// Created by 丁俊 on 2018/7/26.
//

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
#include <ctime>

#include <fastcommon/ini_file_reader.h>
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
#include "frame/sf_config.h"
#include "frame/sf_logger.h"

#include "server/callback.h"
#include "server/version.h"
#include "server/server.h"

#include "ds_conf.h"

#include <fastcommon/logger.h>

#include "common/socket_server.h"
#include "common/socket_session_impl.h"
#include "frame/sf_service.h"
#include "frame/sf_socket_buff.h"
#include "frame/sf_status.h"
#include "frame/sf_util.h"
#include <thread>
#include "mock/master_server.h"

#include "range/range.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "proto/gen/schpb.pb.h"


#ifndef LOG_MACRO_FNAME_
#define LOG_MACRO_FNAME_
#define __FNAME__ "master_server"
#endif


using sharkstore::dataserver::server::GetVersionInfo;

int test_startDS(){
    //start ds
    sf_set_proto_header_size(sizeof(ds_proto_header_t));
    sf_regist_body_length_callback(ds_get_body_length);
    sf_regist_load_config_callback(load_from_conf_file);

    sf_regist_print_version_callback(print_version);

    sf_regist_user_init_callback(ds_user_init_callback);
    sf_regist_user_destroy_callback(ds_user_destroy_callback);

    sf_service_run_test(confStr.c_str());
    return 0;
}

int test_createRange(){
    /**
    sharkstore::dataserver::server::DataServer ds = sharkstore::dataserver::server::DataServer::Instance();
    schpb::CreateRangeRequest req;
    auto meta = req.mutable_range();

    meta->set_id(2);
    meta->set_start_key("01004");
    meta->set_end_key("01005");
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1000);
    peer->set_node_id(1);
    peer->set_type(metapb::PeerType_Normal);

    ds.DealTask(&req);
*/
}

int main() {
//    std::thread ds(test_startDS());


    //start master
    std::thread ms(mspb::startMSRPC);

    sleep(3000);
    test_createRange();
    ms.join();
    FLOG_INFO("ms exit....");
    //ds.join();
    return 0;
}


