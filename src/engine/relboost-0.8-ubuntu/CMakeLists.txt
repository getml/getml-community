# https://stackoverflow.com/questions/17511496/how-to-create-a-shared-library-with-cmake

# Run as follows:
# cmake -H. -Bbuild
# cd build
# make -j8

set(RELBOOST_VERSION relboost-0.8-ubuntu)

cmake_minimum_required(VERSION 2.8)

# Set the path containing poco. 
# Should contain the folder "Net", among others.
set(POCO_DIR $ENV{HOME}/poco-1.9.0-all)

project(${RELBOOST_VERSION}-64bit)

set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR})
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib)

include_directories(
    "${PROJECT_SOURCE_DIR}/../Code/include" 
    "${POCO_DIR}/JSON/include"  
    "${POCO_DIR}/Foundation/include" 
)

# Defines ${OBJ}
set(SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src")
include(${SOURCE_DIRECTORY}/obj.txt)
add_executable(${RELBOOST_VERSION}-64bit-test ${OBJ} ${SOURCE_DIRECTORY}/main.cpp)

add_library(${RELBOOST_VERSION}-64bit SHARED ${OBJ})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++11 -Wall -Wno-sign-compare -Wno-missing-braces -pthread -fno-strict-aliasing -fwrapv -O2 -D NDEBUGLOG")
    
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -O2")

set(
    TARGET_LINK_LIB 
    "${POCO_DIR}/lib/Linux/x86_64/libPocoJSON.a"
    "${POCO_DIR}/lib/Linux/x86_64/libPocoFoundation.a"
)

target_link_libraries(
    ${RELBOOST_VERSION}-64bit
)

target_link_libraries(
    ${RELBOOST_VERSION}-64bit-test
    ${TARGET_LINK_LIB}
)

