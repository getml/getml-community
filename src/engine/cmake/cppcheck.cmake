find_program(CPPCHECK cppcheck)
if(CPPCHECK)
  message(STATUS "Found cppcheck: ${CPPCHECK}. Will be using it to check the code")
  set(CMAKE_CXX_CPPCHECK "${CPPCHECK}" CACHE FILEPATH "Path to cppcheck executable")
else()
  message(STATUS "cppcheck not found. Please install it to check the code")
endif()