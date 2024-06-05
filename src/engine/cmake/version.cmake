function(get_version)
  if(DEFINED ENV{GETML_VERSION})
    set(GETML_VERSION $ENV{GETML_VERSION} CACHE STRING "Version of the getML project" FORCE)
    message(STATUS "Using version ${GETML_VERSION} from environment variable")
    return()
  endif()

  if(EXISTS "../VERSION")
    file(STRINGS "../VERSION" _GETML_VERSION)
    set(GETML_VERSION ${_GETML_VERSION} CACHE STRING "Version of the getML project" FORCE)
    message(STATUS "Using version ${GETML_VERSION} from ../VERSION")
    return()
  endif()

  if(DEFINED CACHE{GETML_VERSION})
    message(STATUS "Using version ${GETML_VERSION} from cache variable")
    return()
  endif()

  set(GETML_VERSION "0.0.0" CACHE STRING "Version of the getML project")
  message(STATUS "Using version ${GETML_VERSION}")

endfunction(get_version)
