# Run as follows:
# cmake -H. -Bbuild
# cd build
# make -j8

set(RELBOOST_VERSION relboost-0.8-macos)

cmake_minimum_required(VERSION 2.8)

# Set the path containing poco. 
# Should contain the folder "Net", among others.
set(POCO_DIR $ENV{HOME}/poco-1.9.0-all)

# Set the path to OpenSSL
set(OPENSSL_DIR /usr/local/opt/openssl)

project(${RELBOOST_VERSION}-64bit)

set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR})
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib)

include_directories(
    "${PROJECT_SOURCE_DIR}/../Code/include" 
    "${POCO_DIR}/Crypto/include"   
    "${POCO_DIR}/JSON/include"  
    "${POCO_DIR}/Net/include" 
    "${POCO_DIR}/NetSSL_OpenSSL/include" 
    "${POCO_DIR}/Foundation/include" 
    "${OPENSSL_DIR}/include"
)

link_directories(${OPENSSL_DIR}/lib)

set(CMAKE_CXX_COMPILER g++-8)

set(ENGINE_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/engine")
set(RELBOOST_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/relboost")

include(${ENGINE_SOURCE_DIRECTORY}/obj.txt)
include(${RELBOOST_SOURCE_DIRECTORY}/obj.txt)

add_executable(${RELBOOST_VERSION}-64bit-test ${RELBOOST_OBJ} ${RELBOOST_SOURCE_DIRECTORY}/main.cpp)

add_executable(${RELBOOST_VERSION}-64bit ${RELBOOST_OBJ} ${ENGINE_OBJ} ${ENGINE_SOURCE_DIRECTORY}/main.cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++11 -Wall -Wno-sign-compare -Wno-missing-braces -pthread -fno-strict-aliasing -fwrapv -O2 -D NDEBUGLOG")
    
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -O2")

target_link_libraries(
    ${RELBOOST_VERSION}-64bit-test
    "${POCO_DIR}/lib/Darwin/x86_64/libPocoJSON.a"
    "${POCO_DIR}/lib/Darwin/x86_64/libPocoFoundation.a"
)

target_link_libraries(
    ${RELBOOST_VERSION}-64bit
    "${POCO_DIR}/lib/Darwin/x86_64/libPocoNetSSL.a"
    "${POCO_DIR}/lib/Darwin/x86_64/libPocoCrypto.a"
    "${POCO_DIR}/lib/Darwin/x86_64/libPocoNet.a"
    "${POCO_DIR}/lib/Darwin/x86_64/libPocoUtil.a"
    "${POCO_DIR}/lib/Darwin/x86_64/libPocoJSON.a"
    "${POCO_DIR}/lib/Darwin/x86_64/libPocoXML.a"
    "${POCO_DIR}/lib/Darwin/x86_64/libPocoFoundation.a"
    crypto
    ssl
)


