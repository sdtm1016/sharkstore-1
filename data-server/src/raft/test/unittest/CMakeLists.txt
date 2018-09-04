add_library(sharkstore-raft-testutil STATIC test_util.cpp)

set(raft_unit_DEPS
    sharkstore-raft-testutil
    sharkstore-raft
    sharkstore-common
    sharkstore-frame
    sharkstore-base
    sharkstore-lkqueue
    sharkstore-proto
    fastcommon
    gtest
    ${PROTOBUF_LIBRARY}
    pthread
)

set (raft_unit_TESTS
    disk_storage_unittest.cpp
    log_file_unittest.cpp
    meta_file_unittest.cpp
    replica_unittest.cpp
    raft_log_unittest.cpp
    raft_types_unittest.cpp
    log_unstable_unittest.cpp
    snapshot_send_unittest.cpp
    snapshot_worker_unittest.cpp
)

ENABLE_TESTING()
foreach(testfile ${raft_unit_TESTS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile})
    target_link_libraries(${exename} ${raft_unit_DEPS})
    add_test(NAME ${exename} COMMAND ${exename})
endforeach(testfile ${TESTS})

