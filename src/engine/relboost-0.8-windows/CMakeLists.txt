# Run as follows:
# cmake -H. -Bbuild
# cd build
# make -j8

set(RELBOOST_VERSION relboost-0.8-windows)

cmake_minimum_required(VERSION 2.8)

# Set the HOME directory (for some reason $ENV{HOME} does not work on Windows.)
set(HOME C:/Users/urban)

# Set the path containing poco.
# Should contain the folder "Net", among others.
set(POCO_DIR ${HOME}/poco-1.9.0-all/poco-1.9.0-all)

# Contains the .lib files for POCO
set(POCO_LIB ${HOME}/CMakeBuilds/7135d9ae-e800-d639-bac7-531036096f67/install/x64-Release/lib)

# Set the path containing xgboost
# Should include folders "include", "lib" and "rabit"
set(XGBOOST_DIR ${HOME}/xgboost)

# Contains the .lib files for xgboost
set(XGBOOST_LIB ${HOME}/xgboost/build/Release)

# Set the path to OpenSSL
set(OPENSSL_DIR ${HOME}/OpenSSL-Win64)

project(${RELBOOST_VERSION}-64bit)

set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR})
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(
    "${PROJECT_SOURCE_DIR}/../Code/include"
    "${POCO_DIR}/Crypto/include"
    "${POCO_DIR}/JSON/include"
    "${POCO_DIR}/Net/include"
    "${POCO_DIR}/NetSSL_OpenSSL/include"
    "${POCO_DIR}/Foundation/include"
    "${OPENSSL_DIR}/include"
    "${XGBOOST_DIR}/include"
    "${XGBOOST_DIR}/rabit/include"
)

link_directories(${OPENSSL_DIR}/lib)

set(AUTOSQL_SOURCE_DIRECTORY ${PROJECT_SOURCE_DIR}/../Code/src/autosql)
set(CSV_SOURCE_DIRECTORY ${PROJECT_SOURCE_DIR}/../Code/src/csv)
set(DATABASE_SOURCE_DIRECTORY ${PROJECT_SOURCE_DIR}/../Code/src/database)
set(ENGINE_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/engine")
set(METRICS_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/metrics")
set(PREDICTORS_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/predictors")
set(RELBOOST_SOURCE_DIRECTORY "${PROJECT_SOURCE_DIR}/../Code/src/relboost")
set(SQLITE3_SOURCE_DIRECTORY ${PROJECT_SOURCE_DIR}/../Code/src/sqlite3)

include(${AUTOSQL_SOURCE_DIRECTORY}/obj.txt)
include(${CSV_SOURCE_DIRECTORY}/obj.txt)
include(${DATABASE_SOURCE_DIRECTORY}/obj.txt)
include(${ENGINE_SOURCE_DIRECTORY}/obj.txt)
include(${METRICS_SOURCE_DIRECTORY}/obj.txt)
include(${PREDICTORS_SOURCE_DIRECTORY}/obj.txt)
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
  ${PREDICTORS_OBJ}
	${RELBOOST_OBJ}
	${SQLITE3_OBJ}
	${ENGINE_SOURCE_DIRECTORY}/main.cpp
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /bigobj /DNDEBUGMSG")

target_link_libraries(
    ${RELBOOST_VERSION}-64bit-autosql-test
    "${POCO_LIB}/PocoNet.lib"
    "${POCO_LIB}/PocoJSON.lib"
    "${POCO_LIB}/PocoFoundation.lib"
)

target_link_libraries(
    ${RELBOOST_VERSION}-64bit-database-test
    "${POCO_LIB}/PocoJSON.lib"
    "${POCO_LIB}/PocoFoundation.lib"
)

target_link_libraries(
    ${RELBOOST_VERSION}-64bit-relboost-test
    "${POCO_LIB}/PocoNet.lib"
    "${POCO_LIB}/PocoJSON.lib"
    "${POCO_LIB}/PocoFoundation.lib"
)

target_link_libraries(
    ${RELBOOST_VERSION}-64bit
    "${POCO_LIB}/PocoNetSSL.lib"
    "${POCO_LIB}/PocoCrypto.lib"
    "${POCO_LIB}/PocoNet.lib"
    "${POCO_LIB}/PocoUtil.lib"
    "${POCO_LIB}/PocoJSON.lib"
    "${POCO_LIB}/PocoXML.lib"
    "${POCO_LIB}/PocoFoundation.lib"
    "${XGBOOST_LIB}/xgboost.lib"
    "${XGBOOST_LIB}/rabit.lib"
)
