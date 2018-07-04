set(test_MOCK_SOURCES
    mock/raft_mock.cpp
    mock/raft_server_mock.cpp
    mock/socket_session_mock.cpp
)

include_directories(${PROJECT_SOURCE_DIR}/test)
include_directories(${PROJECT_SOURCE_DIR}/test/mock)

add_library(test-mock-shared SHARED ${test_MOCK_SOURCES})

set (test_DEPEND_LIBS
    sharkstore-ds-shared
    test-mock-shared
    ${GTEST_LIBRARY}
    ${depend_LIBRARYS}
)

set (TESTS
    encoding_unittest.cpp
    field_value_unittest.cpp
    frame_test_client.cpp
    frame_test_server.cpp
    meta_store_unittest.cpp
    monitor_unittest.cpp
    row_decoder_unittest.cpp
    status_unittest.cpp
    store_unittest.cpp
    util_unittest.cpp
)

foreach(testfile ${TESTS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile})
    target_link_libraries(${exename} ${test_DEPEND_LIBS})
    if("${exename}" MATCHES "unittest")
        add_test(NAME ${exename} COMMAND ${exename})
    endif()
endforeach(testfile ${TESTS})

add_subdirectory(range)

if (BUILD_NET)
    add_subdirectory(net)
endif()

set (FASTS
    fast_net_client.cpp
    fast_net_server.cpp
)

foreach(f IN LISTS FASTS)
    set_source_files_properties(${f} PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"${f}\"")
endforeach()


foreach(testfile ${FASTS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile} gen/test.pb.cc)
    target_link_libraries(${exename} ${test_DEPEND_LIBS})
endforeach(testfile ${FASTS})
