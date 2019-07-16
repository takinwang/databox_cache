cmake_minimum_required (VERSION 3.1.0)

project(DBoxCache)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_INSTALL_PREFIX "/")

# Bump versions on release
 set(DBOXCACHE_VERSION_MAJOR 0)
 set(DBOXCACHE_VERSION_MINOR 1)
 set(DBOXCACHE_VERSION_PATCH 1)
 set(DBOXCACHE_VERSION ${DBOXCACHE_VERSION_MAJOR}.${DBOXCACHE_VERSION_MINOR}.${DBOXCACHE_VERSION_PATCH})

# configure a header file to pass some of the CMake settings
# to the source code
# configure_file (
#  "${PROJECT_SOURCE_DIR}/Config.h.in"
#  "${PROJECT_BINARY_DIR}/Config.h"
# )
 
# add the binary tree to the search path for include files
# so that we will find Config.h
include_directories("${PROJECT_BINARY_DIR}")

# Build options  
option(ENABLE_LOGGER  "Enable log4cplus" OFF)
option(ENABLE_DEBUG   "Enable debug"     OFF) 

if(ENABLE_DEBUG)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb -O0 -D__DEBUG__ -D__TRACE__")	
else()
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g0 -O3 ")		
endif() 

if(NOT ENABLE_LOGGER) 
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__NOLOGGER__")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__NOLOGGER__")

add_compile_options(-Wsign-compare)
add_compile_options(-Wall) 
add_compile_options(-fPIC) 
 
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lpthread")

include_directories ("${PROJECT_SOURCE_DIR}/backends/local")
add_subdirectory (backends/local) 
 
include_directories ("${PROJECT_SOURCE_DIR}/backends/rados")
add_subdirectory (backends/rados) 

include_directories ("${PROJECT_SOURCE_DIR}/dboxcache")
add_subdirectory (dboxcache) 

include_directories ("${PROJECT_SOURCE_DIR}/dboxslab")
add_subdirectory (dboxslab) 

include_directories ("${PROJECT_SOURCE_DIR}/clients")
add_subdirectory (clients) 
