add_subdirectory(commands)
add_subdirectory(communication)
add_subdirectory(containers)
add_subdirectory(database)
add_subdirectory(engine)
add_subdirectory(fastprop)
add_subdirectory(helpers)
add_subdirectory(io)
add_subdirectory(metrics)
add_subdirectory(memmap)
add_subdirectory(predictors)
add_subdirectory(textmining)
add_subdirectory(transpilation)
add_subdirectory(tsindex)

set(
  OBJECT_TARGETS
    commands
    communication
    containers
    database
    engine-core
    fastprop
    helpers
    io
    metrics
    memmap
    predictors
    textmining
    transpilation
    tsindex
)

foreach(
    target
    IN ITEMS
    ${OBJECT_TARGETS}
  )
  target_include_directories(${target} PRIVATE ${CMAKE_SOURCE_DIR}/include)
  target_compile_features(${target} PRIVATE cxx_std_23)
  set_target_properties(${target} PROPERTIES CXX_EXTENSIONS OFF)

  target_compile_definitions(
    ${target}
    PRIVATE
    $<$<BOOL:${HIDE_DEBUG_INFO}>:NDEBUG=1>
    $<$<BOOL:${USE_PROFILING}>:GETML_PROFILING=1>
  )
endforeach()

add_executable(
  engine
  engine/main.cpp
)

if(is_ipo_supported AND ${CMAKE_BUILD_TYPE} STREQUAL "Release")
  set_target_properties(engine PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_compile_features(engine PRIVATE cxx_std_23)
set_target_properties(engine PROPERTIES CXX_EXTENSIONS OFF)

target_include_directories(engine PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(
  engine
  PRIVATE
  $<$<BOOL:${USE_PROFILING}>:PkgConfig::libprofiler>
  $<$<BOOL:${USE_PROFILING}>:PkgConfig::libunwind>
  $<$<BOOL:${OpenMP_CXX_FOUND}>:OpenMP::OpenMP_CXX>

  ${OBJECT_TARGETS}
)

target_compile_definitions(
  engine
  PRIVATE
  $<$<BOOL:${USE_PROFILING}>:GETML_PROFILING=1>
  $<$<NOT:$<BOOL:${EIGEN_PARALLELIZE}>>:EIGEN_DONT_PARALLELIZE=1>
  $<$<BOOL:${HIDE_DEBUG_INFO}>:NDEBUG=1>
)

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
