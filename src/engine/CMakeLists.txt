cmake_minimum_required(VERSION 3.4)

# Find ccache and use it when installed
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    message("Found ccache: ${CCACHE_PROGRAM}. Will be using it as C and CXX Launcher")
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif()

# Set the path containing the Code
set(CODE_DIR $ENV{ENGINE_DIR})

if(NOT CODE_DIR)
    set(CODE_DIR ${CMAKE_CURRENT_LIST_DIR})
endif()

# Set the path containing arrow
set(ARROW_DIR $ENV{ARROW_DIR})

if(NOT ARROW_DIR)
    set(ARROW_DIR ${CODE_DIR}/dependencies/arrow)
endif()

# Set the path containing mariadb
# Should include folders "include" and "build"
set(MARIADB_DIR $ENV{MARIADB_DIR})

if(NOT MARIADB_DIR)
    set(MARIADB_DIR ${CODE_DIR}/dependencies/mariadb-connector-c)
endif()

# Set the path containing postgres
set(POSTGRES_DIR $ENV{POSTGRES_DIR})

if(NOT POSTGRES_DIR)
    set(POSTGRES_DIR ${CODE_DIR}/dependencies/postgres)
endif()

# Set the path containing poco.
# Should contain the folder "Net", among others.
set(POCO_DIR $ENV{POCO_DIR})

if(NOT POCO_DIR)
    set(POCO_DIR ${CODE_DIR}/dependencies/poco)
endif()

# Set the path containing reflect-cpp
set(REFLECTCPP_DIR $ENV{REFLECTCPP_DIR})

if(NOT REFLECTCPP_DIR)
    set(REFLECTCPP_DIR ${CODE_DIR}/dependencies/reflect-cpp)
endif()

# Set the path containing xgboost
# Should include folders "include", "lib" and "rabit"
set(XGBOOST_DIR $ENV{XGBOOST_DIR})

if(NOT XGBOOST_DIR)
   set(XGBOOST_DIR ${CODE_DIR}/dependencies/xgboost)
endif()

# Whether you want to activate the assertions
set(DEBUG_MODE $ENV{DEBUG_MODE})

if(NOT DEBUG_MODE)
    set(DEBUG_MODE "OFF")
endif()

# Whether you want to activate profiling using gperftools
set(PROFILING_MODE $ENV{PROFILING_MODE})

if(NOT PROFILING_MODE)
    set(PROFILING_MODE "OFF")
endif()

project(engine)

set(CMAKE_BINARY_DIR $ENV{CMAKE_BINARY_DIR})
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/bin")
set(LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

include_directories(
    "${CODE_DIR}/include"
    "${ARROW_DIR}/cpp/src"
    "${ARROW_DIR}/cpp/release/src"
    "${MARIADB_DIR}/include"
    "${MARIADB_DIR}/release/include"
    "${POCO_DIR}/Crypto/include"
    "${POCO_DIR}/JSON/include"
    "${POCO_DIR}/Net/include"
    "${POCO_DIR}/NetSSL_OpenSSL/include"
    "${POCO_DIR}/Foundation/include"
    "${POSTGRES_DIR}/release/include"
    "${REFLECTCPP_DIR}/include"
    "${XGBOOST_DIR}/include"
    "${XGBOOST_DIR}/rabit/include"
)

link_directories(
    "${MARIADB_DIR}/release/libmariadb"
    "${XGBOOST_DIR}/lib"
)

include(${CODE_DIR}/src/obj.txt)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -O2")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -Wall -Wno-sign-compare -Wno-missing-braces -pthread -fno-strict-aliasing -fwrapv -ftemplate-backtrace-limit=0 -O2 -DEIGEN_DONT_PARALLELIZE")

if((${PROFILING_MODE} STREQUAL "ON"))
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -DGETML_PROFILING")
    message("WARNING: We are compiling in PROFILING_MODE. Do not release this!")
    message("")
endif()

if(NOT (${DEBUG_MODE} STREQUAL "ON"))
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG")
else()
    message("WARNING: We are compiling in DEBUG_MODE. Do not release this!")
    message("")
endif()

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -s -O2 -fopenmp '-Wl,-rpath,$ORIGIN/../lib'")

target_link_libraries(
    engine
    "${POCO_DIR}/release/build/lib/libPocoNetSSL.a"
    "${POCO_DIR}/release/build/lib/libPocoCrypto.a"
    "${POCO_DIR}/release/build/lib/libPocoNet.a"
    "${POCO_DIR}/release/build/lib/libPocoUtil.a"
    "${POCO_DIR}/release/build/lib/libPocoJSON.a"
    "${POCO_DIR}/release/build/lib/libPocoXML.a"
    "${POCO_DIR}/release/build/lib/libPocoFoundation.a"
    "${POSTGRES_DIR}/release/lib/libpq.a"
    "${POSTGRES_DIR}/release/lib/libpgport.a"
    "${POSTGRES_DIR}/release/lib/libpgcommon.a"
    "${REFLECTCPP_DIR}/build/libreflectcpp.a"
    "${ARROW_DIR}/cpp/release/release/libparquet.a"
    "${ARROW_DIR}/cpp/release/release/libarrow.a"
    "${ARROW_DIR}/cpp/release/release/libarrow_bundled_dependencies.a"
    ssl
    crypto
    "${MARIADB_DIR}/release/libmariadb/libmariadb.so.3"
    xgboost
    dl
    z
    zstd
)

if(${PROFILING_MODE} STREQUAL "ON")
target_link_libraries(
    engine
    profiler
)
endif()

unset(CODE_DIR)
unset(ARROW_DIR)
unset(MARIADB_DIR)
unset(POSTGRES_DIR)
unset(POCO_DIR)
unset(REFLECTCPP_DIR)
unset(XGBOOST_DIR)
unset(DEBUG_MODE)
unset(PROFILING_MODE)
