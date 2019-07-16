 
include_directories("${CMAKE_SOURCE_DIR}/commons")

include_directories("${CMAKE_SOURCE_DIR}/extras/jsoncpp/include")  
include_directories("${CMAKE_SOURCE_DIR}/extras/leveldb/include")  

add_executable(dboxcache
	dboxcache_main.cpp 
	m/CacheFileService.cpp 
	m/CacheMetaManager.cpp 
	m/CacheStatusService.cpp 
	m/MetaLogger.cpp
) 

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__JSON__")	

target_link_libraries(dboxcache  
	boost_system  
	
	/usr/lib64/libdboxcore.a 
	
	${CMAKE_SOURCE_DIR}/extras/jsoncpp/lib/libjsoncpp.a
	${CMAKE_SOURCE_DIR}/extras/leveldb/lib/libleveldb.a
	${CMAKE_SOURCE_DIR}/extras/snappy/lib/libsnappy.a
) 

if(ENABLE_LOGGER)
	target_link_libraries(dboxcache log4cplus) 
endif()

export(TARGETS dboxcache FILE "${CMAKE_BINARY_DIR}/dboxcache-targets.cmake")
  
include (GNUInstallDirs)

install(TARGETS dboxcache
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT RuntimeLibraries
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT Development
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT RuntimeLibraries
)
