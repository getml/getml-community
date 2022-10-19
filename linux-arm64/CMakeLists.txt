#set(GETML_VERSION $ENV{GETML_VERSION})

 cmake_minimum_required(VERSION 3.0)

 # Set the path containing the Code
 set(CODE_DIR $ENV{CODE_DIR})

 # Set the path containing arrow
 set(ARROW_DIR $ENV{ARROW_DIR})

 # Set the path containing mariadb
 # Should include folders "include" and "build"
 set(MARIADB_DIR $ENV{MARIADB_DIR})

 # Set the path containing postgres
 set(POSTGRES_DIR $ENV{POSTGRES_DIR})

 # Set the path containing poco.
 # Should contain the folder "Net", among others.
 set(POCO_DIR $ENV{POCO_DIR})

 # Set the path containing xgboost
 # Should include folders "include", "lib" and "rabit"
 set(XGBOOST_DIR $ENV{XGBOOST_DIR})

 # Whether you want to activate the assertions
 set(DEBUG_MODE $ENV{DEBUG_MODE})

 # Whether you want to activate profiling using gperftools 
 set(PROFILING_MODE $ENV{PROFILING_MODE})

 project(${GETML_VERSION}-engine)

 set(CMAKE_BINARY_DIR $ENV{CMAKE_BINARY_DIR})
 set(EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/bin")
 set(LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

 include_directories(
     "${CODE_DIR}/include"
     "${ARROW_DIR}/cpp/src"
     "${ARROW_DIR}/cpp/release/src"
     "${MARIADB_DIR}/include"
     "${MARIADB_DIR}/build/include"
     "${POCO_DIR}/Crypto/include"
     "${POCO_DIR}/JSON/include"
     "${POCO_DIR}/Net/include"
     "${POCO_DIR}/NetSSL_OpenSSL/include"
     "${POCO_DIR}/Foundation/include"
     "${POSTGRES_DIR}/build/include"
     "${XGBOOST_DIR}/include"
     "${XGBOOST_DIR}/rabit/include"
 )

 link_directories(
     "${MARIADB_DIR}/build/libmariadb"
     "${XGBOOST_DIR}/lib"
 )

 include(${CODE_DIR}/src/obj.txt)

 set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -O2")

 set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -Wall -Wno-sign-compare -Wno-missing-braces -Wno-psabi -pthread -fno-strict-aliasing -fwrapv -O2")

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

 add_definitions(-DGETML_VERSION="${GETML_VERSION}")

 target_link_libraries(
    ${GETML_VERSION}-engine
    "${POCO_DIR}/cmake-build/lib/libPocoNetSSL.a"
    "${POCO_DIR}/cmake-build/lib/libPocoCrypto.a"
    "${POCO_DIR}/cmake-build/lib/libPocoNet.a"
    "${POCO_DIR}/cmake-build/lib/libPocoUtil.a"
    "${POCO_DIR}/cmake-build/lib/libPocoJSON.a"
    "${POCO_DIR}/cmake-build/lib/libPocoXML.a"
    "${POCO_DIR}/cmake-build/lib/libPocoFoundation.a"
    "${POSTGRES_DIR}/build/lib/libpq.a"
    "${POSTGRES_DIR}/build/lib/libpgport.a"
    "${POSTGRES_DIR}/build/lib/libpgcommon.a"
    "${ARROW_DIR}/cpp/release/release/libparquet.a"
    "${ARROW_DIR}/cpp/release/release/libarrow.a"
    "${ARROW_DIR}/cpp/release/release/libarrow_bundled_dependencies.a"
    ssl
    crypto
    mariadb
    xgboost
    dl
    z
    zstd
 )

 if(${PROFILING_MODE} STREQUAL "ON")
 target_link_libraries(
    ${GETML_VERSION}-engine
    profiler
 )
 endif()
