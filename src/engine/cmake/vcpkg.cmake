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

  if(DEFINED ENV{VCPKG_INSTALLED_DIR})
    set(
      VCPKG_INSTALLED_DIR
        $ENV{VCPKG_INSTALLED_DIR}
      CACHE
      PATH
      "Path to vcpkg installed dir"
      FORCE)
    message(STATUS "Using VCPKG_INSTALLED_DIR: ${VCPKG_INSTALLED_DIR}")
  else()
    message(STATUS "VCPKG_INSTALLED_DIR not set. Using default.")
  endif()

  if(DEFINED ENV{VCPKG_INSTALL_OPTIONS})
    set(
      VCPKG_INSTALL_OPTIONS
        $ENV{VCPKG_INSTALL_OPTIONS}
      CACHE
      STRING
      "vcpkg install options"
      FORCE)
    message(STATUS "Using VCPKG_INSTALL_OPTIONS: ${VCPKG_INSTALL_OPTIONS}")
  else()
    message(STATUS "VCPKG_INSTALL_OPTIONS not set. Using default.")
  endif()

  if(DEFINED ENV{VCPKG_TARGET_TRIPLET})
    set(
      VCPKG_TARGET_TRIPLET
        $ENV{VCPKG_TARGET_TRIPLET}
      CACHE
      STRING
      "vcpkg target triplet"
      FORCE)
    message(STATUS "Using VCPKG_TARGET_TRIPLET: ${VCPKG_TARGET_TRIPLET}")
  else()
    message(STATUS "VCPKG_TARGET_TRIPLET not set. Using default.")
  endif()

  set(
    CMAKE_TOOLCHAIN_FILE
      ${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake
    CACHE
    FILEPATH
    "Path to vcpkg toolchain file"
    FORCE)

endfunction(prepare_vcpkg)
