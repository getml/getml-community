add_library(engine-base OBJECT)
add_executable(engine)

if(is_ipo_supported AND ${CMAKE_BUILD_TYPE} STREQUAL "Release")
  set_target_properties(engine-base PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
  set_target_properties(engine PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_compile_features(engine-base PUBLIC cxx_std_23)
set_target_properties(engine-base PROPERTIES CXX_EXTENSIONS OFF)
set_target_properties(engine PROPERTIES CXX_EXTENSIONS OFF)

target_include_directories(engine-base PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(
  engine-base
  PUBLIC
  $<$<BOOL:${USE_PROFILING}>:PkgConfig::libprofiler>
  $<$<BOOL:${USE_PROFILING}>:PkgConfig::libunwind>
  $<$<BOOL:${OpenMP_CXX_FOUND}>:OpenMP::OpenMP_CXX>
  reflectcpp::reflectcpp
  $<$<BOOL:${USE_CONAN}>:yyjson::yyjson>
  Eigen3::Eigen
  range-v3::range-v3
  xgboost::xgboost
  Poco::Foundation
  Poco::Net
  SQLite::SQLite3
  PostgreSQL::PostgreSQL
  libmariadb::libmariadb
  arrow::arrow
  parquet::parquet
)

target_compile_definitions(
  engine-base
  PUBLIC
  $<$<BOOL:${USE_PROFILING}>:GETML_PROFILING=1>
  $<$<NOT:$<BOOL:${EIGEN_PARALLELIZE}>>:EIGEN_DONT_PARALLELIZE=1>
  $<$<BOOL:${HIDE_DEBUG_INFO}>:NDEBUG=1>
)

target_link_libraries(engine PRIVATE engine-base)

set_target_properties(
  engine
  PROPERTIES
    INSTALL_RPATH "$ORIGIN/../lib"
    RUNTIME_OUTPUT_DIRECTORY bin
)
install(
  TARGETS
    engine
  RUNTIME_DEPENDENCIES
    PRE_EXCLUDE_REGEXES
      ".*libpthread\.so.*"
      ".*libm\.so.*"
      ".*libdl\.so.*"
      ".*librt\.so.*"
      ".*libc\.so.*"
      ".*ld-linux-.*\.so.*"
    POST_EXCLUDE_FILES
      ${CPACK_EXCLUDE_RUNTIME_DEPENDENCIES}
    DIRECTORIES
      ${CONAN_RUNTIME_LIB_DIRS}
  RUNTIME
    COMPONENT Runtime
  LIBRARY
    COMPONENT Runtime
)

add_subdirectory(commands)
add_subdirectory(communication)
add_subdirectory(containers)
add_subdirectory(database)
add_subdirectory(engine)
add_subdirectory(fastprop)
add_subdirectory(helpers)
add_subdirectory(io)
add_subdirectory(logging)
add_subdirectory(metrics)
add_subdirectory(memmap)
add_subdirectory(multithreading)
add_subdirectory(optimizers)
add_subdirectory(predictors)
add_subdirectory(strings)
add_subdirectory(textmining)
add_subdirectory(transpilation)
add_subdirectory(tsindex)
