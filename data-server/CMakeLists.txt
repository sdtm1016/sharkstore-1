cmake_minimum_required(VERSION 2.6)
project(sharkstore-ds C CXX)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_CXX_STANDARD 11)

set(CXX_FLAGS
    -std=c++11
    -Wall
    -Wextra
    -Wformat
    -Wsign-compare
    -Wno-unused-parameter
    -Wno-unused-variable
    -Woverloaded-virtual
    -Wnon-virtual-dtor
    -Wno-missing-field-initializers
    -D__STDC_FORMAT_MACROS
    -DASIO_STANDALONE
)

string(REPLACE ";" " " CMAKE_CXX_FLAGS "${CXX_FLAGS}")

set(CMAKE_C_FLAGS "-std=c99 -Wall -Wformat -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE")
if (APPLE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DDARWIN  -DIOEVENT_USE_KQUEUE")
elseif(UNIX)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DOS_LINUX -DIOEVENT_USE_EPOLL")
else()
    message(FATAL_ERROR "Unsupported OS Type: " ${CMAKE_SYSTEM_NAME})
endif()

if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O2 -DNDEBUG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O2 -DNDEBUG" )
    set(CMAKE_C_FLAGS_RELEASE "")
    set(CMAKE_CXX_FLAGS_RELEASE "")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -ggdb -DDEBUG_FLAG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -g -ggdb -DDEBUG_FLAG")
endif()
MESSAGE(STATUS CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})

# use gcov
OPTION (ENABLE_COVERAGE "Use gcov" OFF)
MESSAGE(STATUS ENABLE_COVERAGE=${ENABLE_COVERAGE})
if(ENABLE_COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

# profiler
OPTION (ENABLE_GPERF "Use gperf" OFF)
MESSAGE(STATUS ENABLE_GPERF=${ENABLE_GPERF})
if(ENABLE_GPERF)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_GPERF")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DUSE_GPERF" )
endif()

# gcc address sanitize
OPTION (ENABLE_SANITIZE "Use gcc address sanitize" OFF)
MESSAGE(STATUS ENABLE_SANITIZE=${ENABLE_SANITIZE})
if(ENABLE_SANITIZE )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address" )
endif()

message(STATUS "CMAKE_C_FLAGS: " ${CMAKE_C_FLAGS})
message(STATUS "CMAKE_CXX_FLAGS: " ${CMAKE_CXX_FLAGS})


include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/src/raft/include)

# find protobuf
find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIRS})


# find grpc 
find_library(GRPC_LIB grpc)
if(NOT GRPC_LIB)
    message(FATAL_ERROR "Grpc library not found")
else()
    message(STATUS "Found Grpc library: " ${GRPC_LIB})
endif() 

find_library(GRPC_CPP_LIB grpc++)
if(NOT GRPC_CPP_LIB)
    message(FATAL_ERROR "Grpc++ library not found")
else()
    message(STATUS "Found Grpc C++ library: " ${GRPC_CPP_LIB})
endif() 

# find grpc header
find_path(GRPC_HEADER_PATH grpc/grpc.h)
if(NOT GRPC_HEADER_PATH )
    message(FATAL_ERROR "Grpc headers not found")
else()
    message(STATUS "Found Grpc headers path: " ${GRPC_HEADER_PATH})
    include_directories(${GRPC_HEADER_PATH})
endif() 


# find rocksdb
find_library(ROCKSDB_LIB rocksdb)
if(NOT ROCKSDB_LIB)
    message(FATAL_ERROR "Rocksdb library not found")
else()
    message(STATUS "Found Rocksdb library: " ${ROCKSDB_LIB})
endif() 

find_path(ROCKSDB_HEADER_PATH rocksdb/db.h)
if(NOT ROCKSDB_HEADER_PATH)
    message(FATAL_ERROR "Rocksdb headers not found")
else()
    message(STATUS "Found Rocksdb headers path: " ${ROCKSDB_HEADER_PATH})
    include_directories(${ROCKSDB_HEADER_PATH})
endif() 


# find fastcommon
find_library(FASTCOMMON_LIB fastcommon) 
if(NOT FASTCOMMON_LIB)
    message(FATAL_ERROR "Fastcommon library not found")
else()
    message(STATUS "Found Fastcommon library: " ${FASTCOMMON_LIB})
endif() 

find_path(FASTCOMMON_HEADER_PATH fastcommon/fast_task_queue.h)
if(NOT FASTCOMMON_HEADER_PATH )
    message(FATAL_ERROR "Fastcommon headers not found")
else()
    message(STATUS "Found Fastcommon headers path: " ${FASTCOMMON_HEADER_PATH})
    include_directories(${FASTCOMMON_HEADER_PATH})
endif() 

set(SOURCES
    src/server/main.cpp
    src/server/callback.cpp
    src/server/server.cpp
    src/server/worker.cpp
    src/server/manager.cpp
    src/server/node_address.cpp
    src/server/raft_logger.cpp
    src/server/run_status.cpp
    src/server/range_server.cpp
    src/server/version.cpp
    src/range/range.cpp
    src/range/lock.cpp
    src/range/raw_get.cpp
    src/range/raw_put.cpp
    src/range/raw_delete.cpp
    src/range/select.cpp
    src/range/insert.cpp
    src/range/delete.cpp
    src/range/split.cpp
    src/range/peer.cpp
    src/range/snapshot.cpp
    src/range/kv_funcs.cpp
    src/storage/aggregate_calc.cpp
    src/storage/field_value.cpp
    src/storage/iterator.cpp
    src/storage/meta_store.cpp
    src/storage/metric.cpp
    src/storage/row_decoder.cpp
    src/storage/row_fetcher.cpp
    src/storage/store.cpp
    src/master/client.cpp
    src/master/connection.cpp
    src/master/rpc_types.cpp
    src/master/worker.cpp
    src/monitor/isystemstatus.cpp
    src/monitor/datacacl.cpp
    src/monitor/encodedata.cpp
    src/monitor/rangedata.cpp
)

if (APPLE)
    list(APPEND SOURCES src/monitor/mac_status.cpp)
elseif(UNIX)
    list(APPEND SOURCES src/monitor/linuxstatus.cpp)
else()
    message(FATAL_ERROR "Unsupported OS Type: " ${CMAKE_SYSTEM_NAME})
endif()


add_subdirectory(src/base)
add_subdirectory(src/lk_queue)
add_subdirectory(src/common)
add_subdirectory(src/frame)
add_subdirectory(src/proto)
add_subdirectory(src/raft)
add_subdirectory(src/tool)

OPTION (BUILD_NET "build sharkstore net" OFF)
MESSAGE(STATUS BUILD_NET=${BUILD_NET})
if (BUILD_NET)
    add_subdirectory(src/net)
endif()

set (depend_LIBRARYS
    sharkstore-base
    sharkstore-proto 
    sharkstore-raft
    sharkstore-common
    sharkstore-frame
    sharkstore-lkqueue
    ${PROTOBUF_LIBRARY}
    ${GRPC_LIB}
    ${GRPC_CPP_LIB}
    ${ROCKSDB_LIB}
    ${FASTCOMMON_LIB}
    pthread
    dl
    z
)
if(ENABLE_GPERF)
    list(APPEND depend_LIBRARYS profiler)
endif()

foreach(f IN LISTS SOURCES) 
    # remove "src/" 
    string(SUBSTRING ${f} 4 -1 fname) 
    set_source_files_properties(${f} PROPERTIES 
        COMPILE_DEFINITIONS "__FNAME__=\"${fname}\"") 
endforeach() 

add_executable(data-server ${SOURCES})
target_link_libraries(data-server ${depend_LIBRARYS})

add_custom_target(
    gen-version ALL
    ${CMAKE_COMMAND} -DPROJECT_SOURCE_DIR=${PROJECT_SOURCE_DIR} -DBUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DBUILD_FLAGS=${CMAKE_CXX_FLAGS} -P ${PROJECT_SOURCE_DIR}/cmake/version.cmake
)
add_dependencies(data-server gen-version)

install(TARGETS data-server RUNTIME DESTINATION bin)


OPTION (BUILD_TEST "build sharkstore tests" OFF)
MESSAGE(STATUS BUILD_TEST=${BUILD_TEST})
if (BUILD_TEST)
    enable_testing()
    find_package(GTest REQUIRED)
    include_directories(${GTEST_INCLUDE_DIRS})
    add_library(sharkstore-ds-shared SHARED ${SOURCES})
    add_subdirectory(test)
endif()
