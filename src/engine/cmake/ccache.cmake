function(prepare_ccache)
  set(USE_CCACHE ON CACHE BOOL "Enable ccache if available")
  if(NOT USE_CCACHE)
    message(STATUS "ccache usage disabled.")
    return()
  endif()

  message(STATUS "Enabling usage of ccache.")

  find_program(CCACHE_PROGRAM ccache)
  if(CCACHE_PROGRAM)
    message(
      STATUS
      "Found ccache: ${CCACHE_PROGRAM}. Will be using it as C and CXX Launcher")
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}" CACHE FILEPATH "Path to C compiler launcher")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}" CACHE FILEPATH "Path to C++ compiler launcher")
  else()
    message(STATUS "ccache not found. Please install it to speed up the compilation")
  endif()

endfunction(prepare_ccache)
