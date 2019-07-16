
include_directories("${CMAKE_SOURCE_DIR}/backends") 
include_directories("${CMAKE_SOURCE_DIR}/commons") 
 
add_executable(dboxslab 
	dboxslab_main.cpp		
	backend/BackendManager.cpp
	w/SlabFileService.cpp
	w/SlabFileManager.cpp
	memory/SlabMemManager.cpp
	memory/SlabMMapManager.cpp
) 

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lpthread")	
    
target_link_libraries(dboxslab 
	dl 
	boost_system 
	
	/usr/lib64/libdboxcore.a 
) 

if(ENABLE_LOGGER)
	target_link_libraries(dboxslab log4cplus) 
endif()

export(TARGETS dboxslab FILE "${CMAKE_BINARY_DIR}/dboxslab-targets.cmake")
  
include (GNUInstallDirs)

install(TARGETS dboxslab
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT RuntimeLibraries
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT Development
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT RuntimeLibraries
)

install(
    FILES "${CMAKE_CURRENT_SOURCE_DIR}/dboxslab.conf" "${CMAKE_CURRENT_SOURCE_DIR}/dboxslab.logger" 
    DESTINATION "${CMAKE_INSTALL_SYSCONFDIR}"
)