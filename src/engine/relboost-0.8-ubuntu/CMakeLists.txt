# Run as follows:
# cmake -H. -Bbuild
# cd build
# make -j8

# To check for memory leaks:
# valgrind --leak-check=full --show-leak-kinds=all -v ./relboost-0.8-ubuntu-64bit

set(RELBOOST_VERSION relboost-0.8-ubuntu)

cmake_minimum_required(VERSION 2.8)

# Set the path containing poco.
# Should contain the folder "Net", among others.
set(POCO_DIR $ENV{HOME}/poco-1.9.0-all)

# Set the path containing xgboost
# Should include folders "include", "lib" and "rabit"
set(XGBOOST_DIR $ENV{HOME}/xgboost)

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
    "${XGBOOST_DIR}/include"
    "${XGBOOST_DIR}/rabit/include"
)

set(AUTOSQL_SOURCE_DIRECTORY ${PROJECT_SOURCE_DIR}/../Code/src/autosql)
set(CSV_SOURCE_DIRECTORY ${PROJECT_SOURCE_DIR}/../Code/src/csv)
set(DATABASE_SOURCE_DIRECTORY ${PROJECT_SOURCE_DIR}/../Code/src/database)
set(ENGINE_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/engine")
set(METRICS_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/metrics")
set(RELBOOST_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/relboost")
set(SQLITE3_SOURCE_DIRECTORY ${PROJECT_SOURCE_DIR}/../Code/src/sqlite3)

include(${AUTOSQL_SOURCE_DIRECTORY}/obj.txt)
include(${CSV_SOURCE_DIRECTORY}/obj.txt)
include(${DATABASE_SOURCE_DIRECTORY}/obj.txt)
include(${ENGINE_SOURCE_DIRECTORY}/obj.txt)
include(${METRICS_SOURCE_DIRECTORY}/obj.txt)
include(${RELBOOST_SOURCE_DIRECTORY}/obj.txt)
include(${SQLITE3_SOURCE_DIRECTORY}/obj.txt)

add_executable(
	${RELBOOST_VERSION}-64bit-autosql-test
	${AUTOSQL_OBJ}
	${AUTOSQL_SOURCE_DIRECTORY}/main.cpp
)

add_executable(
	${RELBOOST_VERSION}-64bit-database-test
	${CSV_OBJ}
	${DATABASE_OBJ}
	${SQLITE3_OBJ}
	${PROJECT_SOURCE_DIR}/../Code/src/database/main.cpp
)

add_executable(
	${RELBOOST_VERSION}-64bit-relboost-test
	${RELBOOST_OBJ}
	${METRICS_OBJ}
	${RELBOOST_SOURCE_DIRECTORY}/main.cpp
)

add_executable(
 	${RELBOOST_VERSION}-64bit
 	${AUTOSQL_OBJ}
 	${CSV_OBJ}
 	${DATABASE_OBJ}
 	${ENGINE_OBJ}
 	${METRICS_OBJ}
 	${RELBOOST_OBJ}
 	${SQLITE3_OBJ}
 	${ENGINE_SOURCE_DIRECTORY}/main.cpp
)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -O2")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++11 -Wall -Wno-sign-compare -Wno-missing-braces -pthread -fno-strict-aliasing -fwrapv -O2 -D NDEBUGLOG")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -s -O2 -fopenmp '-Wl,-rpath,$ORIGIN/../lib'")

target_link_libraries(
    ${RELBOOST_VERSION}-64bit-autosql-test
    "${POCO_DIR}/lib/Linux/x86_64/libPocoNet.a"
    "${POCO_DIR}/lib/Linux/x86_64/libPocoJSON.a"
    "${POCO_DIR}/lib/Linux/x86_64/libPocoFoundation.a"
)

target_link_libraries(
    ${RELBOOST_VERSION}-64bit-database-test
	${CMAKE_DL_LIBS}
    "${POCO_DIR}/lib/Linux/x86_64/libPocoJSON.a"
    "${POCO_DIR}/lib/Linux/x86_64/libPocoFoundation.a"
	pthread
	dl
)

target_link_libraries(
    ${RELBOOST_VERSION}-64bit-relboost-test
    "${POCO_DIR}/lib/Linux/x86_64/libPocoNet.a"
    "${POCO_DIR}/lib/Linux/x86_64/libPocoJSON.a"
    "${POCO_DIR}/lib/Linux/x86_64/libPocoFoundation.a"
)

target_link_libraries(
   ${RELBOOST_VERSION}-64bit
   "${POCO_DIR}/lib/Linux/x86_64/libPocoNetSSL.a"
   "${POCO_DIR}/lib/Linux/x86_64/libPocoCrypto.a"
   "${POCO_DIR}/lib/Linux/x86_64/libPocoNet.a"
   "${POCO_DIR}/lib/Linux/x86_64/libPocoUtil.a"
   "${POCO_DIR}/lib/Linux/x86_64/libPocoJSON.a"
   "${POCO_DIR}/lib/Linux/x86_64/libPocoXML.a"
   "${POCO_DIR}/lib/Linux/x86_64/libPocoFoundation.a"
   "${XGBOOST_DIR}/lib/libxgboost.a"
   "${XGBOOST_DIR}/rabit/lib/librabit.a"
   "${XGBOOST_DIR}/dmlc-core/libdmlc.a"
   ssl
   crypto
   dl
)
