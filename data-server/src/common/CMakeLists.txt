set(common_SOURCES
    ds_config.c
    ds_proto.c
    ds_version.c
    ds_encoding.cpp
    socket_session_impl.cpp
    socket_base.cpp
    socket_message.cpp
    socket_server.cpp
    socket_client.cpp
    )

foreach(f IN LISTS common_SOURCES) 
    set_source_files_properties(${f} PROPERTIES 
        COMPILE_DEFINITIONS "__FNAME__=\"common/${f}\"") 
endforeach() 

add_library(sharkstore-common STATIC ${common_SOURCES})
