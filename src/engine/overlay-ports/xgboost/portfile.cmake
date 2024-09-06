vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO dmlc/xgboost
    REF "v${VERSION}"
    SHA512 4465f383df70ee415faaeb745459bfc413f71bff0d02e59e67173975188bed911044dbea1a456550496f8c5a7c0b50002275b6be72c87386a6118485e1e41829
    HEAD_REF master
)

################################################################################
# Load submodule: dmlc/dmlc-core

vcpkg_from_github(
    OUT_SOURCE_PATH DMLC_CORE_SOURCE_PATH
    REPO dmlc/dmlc-core
    REF 81db539486ce6525b31b971545edffee2754aced
    SHA512 9b288fd1ceeef0015e80b0296b0d4015238d4cc1b7c36ba840d3eabce87508e62ed5b4fe61504f569dadcc414882903211fadf54aa0e162a896b03d7ca05e975
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
# Load submodule: NVlabs/cub

vcpkg_from_github(
    OUT_SOURCE_PATH CUB_SOURCE_PATH
    REPO NVlabs/cub
    REF af39ee264f4627608072bf54730bf3a862e56875
    SHA512 bba9099e38833cd59900567cf5c401ba4337b96ccfd67f1b02f606ea9f4ff9512a8941b9ef7d8629acf7f32567969ed60ee6ebbe72b9d30ddacad36347d03f1e
    HEAD_REF master
)

file(REMOVE_RECURSE "${SOURCE_PATH}/cub")
file(RENAME "${CUB_SOURCE_PATH}" "${SOURCE_PATH}/cub")

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
