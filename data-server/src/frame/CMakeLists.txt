set(frame_SOURCES
    sf_config.c
    sf_service.c
    sf_socket_buff.c
    sf_socket.c
    sf_socket_session.c
    sf_socket_thread.c
    sf_status.c
    sf_util.c
    )

foreach(f IN LISTS frame_SOURCES) 
    set_source_files_properties(${f} PROPERTIES 
        COMPILE_DEFINITIONS "__FNAME__=\"frame/${f}\"") 
endforeach() 

add_library(sharkstore-frame STATIC ${frame_SOURCES})
