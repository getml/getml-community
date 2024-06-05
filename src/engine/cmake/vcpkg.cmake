function(prepare_vcpkg)
  set(
    USE_VCPKG
      OFF
    CACHE
    BOOL
    "Use vcpkg to install dependencies")

  if(NOT USE_VCPKG)
    message(STATUS "Not using vcpkg to install dependencies.")
    return()
  endif()

  message(STATUS "Using vcpkg to install dependencies.")

  if(DEFINED ENV{VCPKG_ROOT})
    set(
      VCPKG_ROOT
        $ENV{VCPKG_ROOT}
      CACHE
      PATH
      "Path to vcpkg root"
      FORCE)
  else()
    set(
      VCPKG_ROOT
      "VCPKG_ROOT_NOT_FOUND"
      CACHE
      PATH
      "Path to vcpkg root")
  endif()

  if(NOT EXISTS ${VCPKG_ROOT})
    message(FATAL_ERROR "Variable VCPKG_ROOT must be set when using vcpkg via USE_VCPKG=ON.")
    return()
  endif()

  message(STATUS "Using VCPKG_ROOT: ${VCPKG_ROOT}")

  set(
    CMAKE_TOOLCHAIN_FILE
      ${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake
    CACHE
    FILEPATH
    "Path to vcpkg toolchain file"
    FORCE)

endfunction(prepare_vcpkg)
