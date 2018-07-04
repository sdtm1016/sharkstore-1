set(net_TESTS
    net_test1.cpp
)

foreach(f IN LISTS net_TESTS) 
    set_source_files_properties(${f} PROPERTIES 
        COMPILE_DEFINITIONS "__FNAME__=\"test/${f}\"") 
endforeach() 

foreach(testfile ${net_TESTS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile})
     target_link_libraries(${exename} sharkstore-net sharkstore-base sharkstore-frame fastcommon glog pthread)
endforeach(testfile ${net_TESTS})
