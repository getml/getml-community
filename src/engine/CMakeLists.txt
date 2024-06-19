cmake_minimum_required(VERSION 3.25)

include(cmake/vcpkg.cmake)
prepare_vcpkg()

include(cmake/conan.cmake)
prepare_conan()

include(cmake/version.cmake)
get_version()

project(
  engine
  VERSION ${GETML_VERSION}
  DESCRIPTION "Engine for GetML"
  HOMEPAGE_URL "https://www.getml.com/"
  LANGUAGES CXX
)

include(cmake/ccache.cmake)
prepare_ccache()

include(cmake/static_analysis.cmake)
prepare_static_analysis()

set(HIDE_DEBUG_INFO ON CACHE BOOL "Hide extra debug information")
set(USE_PROFILING OFF CACHE BOOL "Enable gperftools profiling settings")
if(USE_PROFILING)
  message(STATUS "gperftools profiling enabled.")
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(libprofiler REQUIRED IMPORTED_TARGET libprofiler)
  pkg_check_modules(libunwind REQUIRED IMPORTED_TARGET libunwind)
else()
  message(STATUS "gperftools profiling disabled.")
endif()

set(EIGEN_PARALLELIZE ON CACHE BOOL "Enable Eigen parallelization")

include(CheckIPOSupported)
check_ipo_supported(RESULT is_ipo_supported)

find_package(Poco REQUIRED Foundation Net)
find_package(PostgreSQL REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(Arrow REQUIRED)
find_package(Eigen3 REQUIRED)
find_package(OpenMP REQUIRED)
find_package(xgboost REQUIRED)


if(NOT ${USE_VCPKG} AND NOT ${USE_CONAN})

  message(STATUS "Using system packages")

  find_package(PkgConfig REQUIRED)

  message(STATUS "-> mariadb")
  pkg_check_modules(libmariadb REQUIRED IMPORTED_TARGET libmariadb)
  add_library(libmariadb::libmariadb ALIAS PkgConfig::libmariadb)

  message(STATUS "-> Arrow")
  if(${ARROW_BUILD_STATIC})
    add_library(arrow::arrow ALIAS Arrow::arrow_static)
  else()
    add_library(arrow::arrow ALIAS Arrow::arrow_shared)
  endif()

  message(STATUS "-> Parquet")
  find_package(Parquet REQUIRED)
  if(${ARROW_BUILD_STATIC})
    add_library(parquet::parquet ALIAS Parquet::parquet_static)
  else()
    add_library(parquet::parquet ALIAS Parquet::parquet_shared)
  endif()

  message(STATUS "-> reflect-cpp")
  find_package(reflectcpp REQUIRED)

elseif(${USE_VCPKG})

  message(STATUS "Using vcpkg packages")

  message(STATUS "-> unofficial mariadb")
  find_package(unofficial-libmariadb REQUIRED)
  add_library(libmariadb::libmariadb ALIAS unofficial::libmariadb)

  message(STATUS "-> Arrow")
  if(${ARROW_BUILD_STATIC})
    add_library(arrow::arrow ALIAS Arrow::arrow_static)
  else()
    add_library(arrow::arrow ALIAS Arrow::arrow_shared)
  endif()

  message(STATUS "-> Parquet")
  find_package(Parquet REQUIRED)
  if(${ARROW_BUILD_STATIC})
    add_library(parquet::parquet ALIAS Parquet::parquet_static)
  else()
    add_library(parquet::parquet ALIAS Parquet::parquet_shared)
  endif()

  message(STATUS "-> reflect-cpp")
  find_package(reflectcpp REQUIRED)

elseif(${USE_CONAN})

  message(STATUS "Using Conan packages")

  message(STATUS "-> mariadb")
  find_package(mariadb-connector-c REQUIRED)
  add_library(libmariadb::libmariadb ALIAS mariadb-connector-c::mariadb-connector-c)

  message(STATUS "-> Parquet")
  add_library(parquet::parquet ALIAS Parquet::parquet_static)

  message(STATUS "-> reflect-cpp")
  find_package(reflect-cpp REQUIRED)
  add_library(reflectcpp::reflectcpp ALIAS reflect-cpp::reflect-cpp)
  find_package(yyjson REQUIRED)

else()
  message(FATAL_ERROR "No package manager specified. Please set either USE_VCPKG or USE_CONAN, or none to use system packages.")
endif()

add_subdirectory(src)

include(InstallRequiredSystemLibraries)

set(CPACK_PACKAGE_VENDOR "https://www.code17.io/")
set(CPACK_PACKAGE_DIRECTORY "package")
set(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${PROJECT_VERSION}-${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}")
set(CPACK_THREADS 0)
include(CPack)
