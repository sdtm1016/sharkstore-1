set(meta_dump_SRCS
    ../src/storage/meta_store.cpp
    meta_dump/meta_dump.cpp
)
add_executable(meta_dump ${meta_dump_SRCS})
set (meta_dump_DEPS
    sharkstore-proto
    sharkstore-base
    ${PROTOBUF_LIBRARY}
    ${ROCKSDB_LIB}
    pthread
)
target_link_libraries(meta_dump ${meta_dump_DEPS})


set(log_dump_SRCS
    log_dump/log_dump.cpp
)
add_executable(log_dump ${log_dump_SRCS})
set (log_dump_DEPS
    sharkstore-raft
    sharkstore-proto
    sharkstore-base
    ${PROTOBUF_LIBRARY}
    pthread
)
target_link_libraries(log_dump ${log_dump_DEPS})
