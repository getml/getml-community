function(prepare_static_analysis)
  set(USE_STATIC_ANALYSIS OFF CACHE BOOL "Enable static analysis")
  if(NOT USE_STATIC_ANALYSIS)
    message(STATUS "Static analysis disabled.")
    return()
  endif()

  message(STATUS "Static analysis enabled.")

  include(cmake/clang_tidy.cmake)
  include(cmake/cppcheck.cmake)
  include(cmake/cpplint.cmake)
  include(cmake/iwyu.cmake)

endfunction(prepare_static_analysis)
