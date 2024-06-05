vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO dmlc/xgboost
    REF "v${VERSION}"
    SHA512 0fef4dd49750c829a69e42fe75872dd88f6e9bbdd6b6912fd5ef77c4f850451de2b09b61da75ce1a789a98f38e2e847919ccf8a477ded7380024d9846be18b80
    HEAD_REF master
)

################################################################################
# Load submodule: dmlc/dmlc-core

vcpkg_from_github(
    OUT_SOURCE_PATH DMLC_CORE_SOURCE_PATH
    REPO dmlc/dmlc-core
    REF ea21135fbb141ae103fb5fc960289b5601b468f2
    SHA512 01b0c0ddddbaeecbe1759225f27da143b87f79cae805bf0227634f382b6cb6e92bcee376f999161c15b3b6a9da227f8c3f022d84bdbbe6589282965cb285a27c
    HEAD_REF master
)

file(REMOVE_RECURSE "${SOURCE_PATH}/dmlc-core")
file(RENAME "${DMLC_CORE_SOURCE_PATH}" "${SOURCE_PATH}/dmlc-core")

################################################################################
# Load submodule: rapidsai/gputreeshap

vcpkg_from_github(
    OUT_SOURCE_PATH GPUTREESHAP_SOURCE_PATH
    REPO rapidsai/gputreeshap
    REF 787259b412c18ab8d5f24bf2b8bd6a59ff8208f3
    SHA512 a3bac6215317a2b5a40bbdd9f5e93a7878ae9fc11d84762db4ebb2e691a7097113cfe5268998878859ad593d7a174a28ca6473e08ba7939da422621677e6879e
    HEAD_REF master
)

file(REMOVE_RECURSE "${SOURCE_PATH}/gputreeshap")
file(RENAME "${GPUTREESHAP_SOURCE_PATH}" "${SOURCE_PATH}/gputreeshap")

################################################################################
# Configure and Install: xgboost

string(COMPARE EQUAL ${VCPKG_LIBRARY_LINKAGE} "static" BUILD_STATIC_LIB)

vcpkg_cmake_configure(
    SOURCE_PATH ${SOURCE_PATH}
    OPTIONS
        ${FEATURE_OPTIONS}
        -DBUILD_C_DOC=OFF
        -DUSE_OPENMP=ON
        -DBUILD_STATIC_LIB=${BUILD_STATIC_LIB}
        -DFORCE_SHARED_CRT=OFF
        -DJVM_BINDINGS=OFF
        -DR_LIB=OFF
        -DUSE_DEBUG_OUTPUT=OFF
        -DFORCE_COLORED_OUTPUT=OFF
        -DENABLE_ALL_WARNINGS=OFF
        -DLOG_CAPI_INVOCATION=OFF
        -DGOOGLE_TEST=OFF
        -DUSE_DMLC_GTEST=OFF
        -DUSE_DEVICE_DEBUG=OFF
        -DUSE_NVTX=OFF
        -DRABIT_MOCK=OFF
        -DHIDE_CXX_SYMBOLS=OFF
        -DKEEP_BUILD_ARTIFACTS_IN_BINARY_DIR=OFF
        -DUSE_CUDA=OFF
        -DUSE_PER_THREAD_DEFAULT_STREAM=ON
        -DUSE_NCCL=OFF
        -DBUILD_WITH_SHARED_NCCL=OFF
        -DUSE_SANITIZER=OFF
        -DPLUGIN_RMM=OFF
        -DPLUGIN_FEDERATED=OFF
        -DADD_PKGCONFIG=OFF
        MAYBE_UNUSED_VARIABLES
        -DBUILD_DEPRECATED_CLI=OFF
        -DPLUGIN_SYCL=OFF
        -DUSE_DLOPEN_NCCL=OFF
)

vcpkg_cmake_install()

vcpkg_cmake_config_fixup(
    CONFIG_PATH "lib/cmake/${PORT}"
)

file(REMOVE_RECURSE
    "${CURRENT_PACKAGES_DIR}/debug/include"
    "${CURRENT_PACKAGES_DIR}/debug/share"
    "${CURRENT_PACKAGES_DIR}/debug/lib/cmake"
)

file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)

find_program(PORT_EXE ${CURRENT_PACKAGES_DIR}/bin/${PORT}.exe)
if(PORT_EXE)
    file(INSTALL "${PORT_EXE}" DESTINATION "${CURRENT_PACKAGES_DIR}/tools/${PORT}")
endif()

find_program(PORT_BIN ${CURRENT_PACKAGES_DIR}/bin/${PORT})
if(PORT_BIN)
    file(INSTALL "${PORT_BIN}" DESTINATION "${CURRENT_PACKAGES_DIR}/tools/${PORT}")
endif()

configure_file("${CMAKE_CURRENT_LIST_DIR}/usage" "${CURRENT_PACKAGES_DIR}/share/${PORT}/usage" COPYONLY)
