
include (GNUInstallDirs)
 
#install(FILES
#	c++/CacheClient.hpp 
#    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/databox"
#    COMPONENT Development
#)

add_executable(testit  
	c++/dboxslab_test_main.cpp 
	c++/CacheClient.cpp
) 

target_link_libraries(testit  
	
	/usr/lib64/libdboxcore.a 
	/usr/lib64/libboost_system.a
) 