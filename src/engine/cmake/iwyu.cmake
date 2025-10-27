find_program(INCLUDE_WHAT_YOU_USE include-what-you-use)
if(INCLUDE_WHAT_YOU_USE)
  message(
    STATUS
    "Found include-what-you-use: ${INCLUDE_WHAT_YOU_USE}. Will be using it to check the code"
  )
  set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE "${INCLUDE_WHAT_YOU_USE}" CACHE FILEPATH "Path to include-what-you-use executable")
else()
  message(STATUS "include-what-you-use not found. Please install it to check the code")
endif()
