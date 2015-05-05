cmake_minimum_required(VERSION 3.0)

#project name
project (DBMS_Recovery)
SET_PROPERTY(GLOBAL PROPERTY USE_FOLDERS ON)

#output path
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)

#gcc flags
#use the following line to build debug mode
#set( CMKAE_CXX_FLAGS "-Wall -Wextra -Werror -g3" )
#use the following line to build release mode
#set( CMAKE_CXX_FLAGS "-o3")
#do not use the line above if not building with gcc

#include directory
include_directories( ${CMAKE_SOURCE_DIR}/header )
file(GLOB HEADERS ${CMAKE_SOURCE_DIR}/header/*.h )

#Subdirectories
add_subdirectory( src )