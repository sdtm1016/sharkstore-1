#include <iostream>
#include <thread>

#include "common/ds_proto.h"
#include "config.h"
#include "frame/sf_service.h"

namespace sharkstore {
namespace raft {
namespace bench {

static int user_init() { return 0; }
void user_destroy() {}

static int load_conf_file(IniContext *ini_context, const char *filename) {
    const char *bench_section = "bench";

    bench_config.request_num =
        iniGetInt64Value(bench_section, "request_num", ini_context, 1000000);
    std::cout << "request num: " << bench_config.request_num << std::endl;

    bench_config.thread_num = iniGetIntValue(bench_section, "thread_num", ini_context, 4);
    std::cout << "bench thread num: " << bench_config.thread_num << std::endl;

    bench_config.range_num = iniGetIntValue(bench_section, "range_num", ini_context, 4);
    std::cout << "bench range num: " << bench_config.range_num << std::endl;

    bench_config.concurrency =
        iniGetIntValue(bench_section, "concurrency", ini_context, 500);
    std::cout << "bench concurrency per thread: " << bench_config.concurrency
              << std::endl;

    const char *raft_section = "raft";
    bench_config.use_memory_raft_log =
        iniGetBoolValue(raft_section, "use_memory_raft_log", ini_context, false);
    std::cout << "use memory raft log: " << bench_config.use_memory_raft_log << std::endl;

    bench_config.use_inprocess_transport =
        iniGetBoolValue(raft_section, "use_inprocess_transport", ini_context, false);
    std::cout << "use inprocess transport: " << bench_config.use_inprocess_transport
              << std::endl;

    bench_config.raft_thread_num =
        iniGetIntValue(raft_section, "raft_thread_num", ini_context, 1);
    std::cout << "raft worker thread num: " << bench_config.raft_thread_num << std::endl;

    bench_config.apply_thread_num =
        iniGetIntValue(raft_section, "apply_thread_num", ini_context, 1);
    std::cout << "raft apply thread num: " << bench_config.apply_thread_num << std::endl;

    return 0;
}

void start_fast_service(int argc, char *argv[]) {
    sf_set_proto_header_size(sizeof(ds_proto_header_t));
    sf_regist_body_length_callback(ds_get_body_length);
    sf_regist_load_config_callback(load_conf_file);
    sf_regist_user_init_callback(user_init);
    sf_regist_user_destroy_callback(user_destroy);

    std::thread t([argc, argv] { sf_service_run(argc, argv, "raft-bench"); });
    t.detach();

    sleep(2);
}

} /* namespace bench */
} /* namespace raft */
} /* namespace sharkstore */
