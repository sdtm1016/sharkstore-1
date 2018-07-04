set(lkqueue_SOURCES
    lk_queue.cpp
    )

foreach(f IN LISTS lkqueue_SOURCES) 
    set_source_files_properties(${f} PROPERTIES 
        COMPILE_DEFINITIONS "__FNAME__=\"common/${f}\"") 
endforeach() 

add_library(sharkstore-lkqueue STATIC ${lkqueue_SOURCES})
