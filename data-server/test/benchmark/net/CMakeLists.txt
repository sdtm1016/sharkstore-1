set(net_bench_DEPS
   sharkstore-proto
   sharkstore-net
   sharkstore-common
   sharkstore-frame
   sharkstore-base
   sharkstore-lkqueue
   ${PROTOBUF_LIBRARY}
   ${FASTCOMMON_LIB}
   pthread
   z
   )

add_executable(bench_client bench_client.cpp)
target_link_libraries(bench_client ${net_bench_DEPS})

add_executable(asio_bench_server asio_bench_server.cpp)
set_source_files_properties(asio_bench_server.cpp PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"asio_bench_server.cpp\"")
target_link_libraries(asio_bench_server ${net_bench_DEPS})

add_executable(fast_bench_server fast_bench_server.cpp)
set_source_files_properties(fast_bench_server.cpp PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"fast_bench_server.cpp\"")
target_link_libraries(fast_bench_server ${net_bench_DEPS})
