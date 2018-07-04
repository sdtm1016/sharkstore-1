set(net_SOURCES
    io_context_pool.cpp
    rpc_client.cpp
    rpc_protocol.cpp
    server.cpp
    session.cpp
    )

foreach(f IN LISTS net_SOURCES) 
    set_source_files_properties(${f} PROPERTIES 
        COMPILE_DEFINITIONS "__FNAME__=\"net/${f}\"") 
endforeach() 

add_library(sharkstore-net STATIC ${net_SOURCES})
