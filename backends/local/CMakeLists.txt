
include_directories("${CMAKE_SOURCE_DIR}/backends") 

add_library(dboxslab_fusebackend 
	SHARED 
    local_backend.cpp      
) 
   
set_target_properties(dboxslab_fusebackend PROPERTIES
    VERSION ${DBOXCACHE_VERSION}
    SOVERSION ${DBOXCACHE_VERSION_MAJOR}
)


# set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libgcc -static-libstdc++")
 
export(TARGETS dboxslab_fusebackend FILE "${CMAKE_BINARY_DIR}/dboxslab_fusebackend-targets.cmake")
  
include (GNUInstallDirs)

install(TARGETS dboxslab_fusebackend
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT RuntimeLibraries
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT Development
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT RuntimeLibraries
)

install(
    FILES "${CMAKE_CURRENT_SOURCE_DIR}/local.conf"
    DESTINATION "${CMAKE_INSTALL_SYSCONFDIR}/dboxslab"
)