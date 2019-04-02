# https://stackoverflow.com/questions/17511496/how-to-create-a-shared-library-with-cmake

# Run as follows:
# cmake -H. -Bbuild
# cd build
# make -j8

set(RELBOOST_VERSION relboost-0.8-ubuntu)

cmake_minimum_required(VERSION 2.8)
/Users/patrickurbanke/relboost-engine/relboost-0.8-ubuntu/CMakeLists.txt
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

set(ENGINE_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/engine")
set(RELBOOST_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/relboost")

include(${ENGINE_SOURCE_DIRECTORY}/obj.txt)
include(${RELBOOST_SOURCE_DIRECTORY}/obj.txt)

add_executable(${RELBOOST_VERSION}-64bit-relboost-test ${RELBOOST_OBJ} ${RELBOOST_SOURCE_DIRECTORY}/main.cpp)

add_executable(${RELBOOST_VERSION}-64bit ${RELBOOST_OBJ} ${ENGINE_OBJ} ${RELBOOST_SOURCE_DIRECTORY}/main.cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++11 -Wall -Wno-sign-compare -Wno-missing-braces -pthread -fno-strict-aliasing -fwrapv -O2 -D NDEBUGLOG")
    
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -O2")

set(
    TARGET_LINK_LIB 
    "${POCO_DIR}/lib/Linux/x86_64/libPocoJSON.a"
    "${POCO_DIR}/lib/Linux/x86_64/libPocoFoundation.a"
)

target_link_libraries(
    ${RELBOOST_VERSION}-64bit-relboost-test
    ${TARGET_LINK_LIB}
)

target_link_libraries(
    ${RELBOOST_VERSION}-64bit
    ${TARGET_LINK_LIB}
)

