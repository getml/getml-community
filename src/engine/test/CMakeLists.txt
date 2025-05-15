file(GLOB_RECURSE TEST_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_SOURCE_DIR}/test/unit/**/*.cpp
)

add_executable(unit_tests ${TEST_SOURCES})

target_include_directories(
    unit_tests
    PRIVATE
    ${CMAKE_SOURCE_DIR}/test
)
target_compile_features(unit_tests PRIVATE cxx_std_23)
set_target_properties(unit_tests PROPERTIES CXX_EXTENSIONS OFF)

target_compile_options(unit_tests PRIVATE --coverage)
target_link_options(unit_tests PRIVATE --coverage)

target_link_libraries(
    unit_tests
    engine-base
    GTest::gtest_main
)

gtest_discover_tests(unit_tests)
