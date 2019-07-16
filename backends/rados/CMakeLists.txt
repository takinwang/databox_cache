
include_directories("${CMAKE_SOURCE_DIR}/backends") 
 
add_library(dboxslab_radosbackend 
	SHARED 
    rados_backend.cpp      
) 
   
target_link_libraries(dboxslab_radosbackend rados )
   
set_target_properties(dboxslab_radosbackend PROPERTIES
    VERSION ${DBOXCACHE_VERSION}
    SOVERSION ${DBOXCACHE_VERSION_MAJOR}
)
 
export(TARGETS dboxslab_radosbackend FILE "${CMAKE_BINARY_DIR}/dboxslab_radosbackend-targets.cmake")
  
include (GNUInstallDirs)

install(TARGETS dboxslab_radosbackend
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT RuntimeLibraries
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT Development
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT RuntimeLibraries
)

install(
    FILES "${CMAKE_CURRENT_SOURCE_DIR}/rados.conf"
    DESTINATION "${CMAKE_INSTALL_SYSCONFDIR}/dboxslab"
)