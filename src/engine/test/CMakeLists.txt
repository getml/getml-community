add_executable(unit_tests)

add_subdirectory(unit)

target_include_directories(
    unit_tests
    PRIVATE
    ${CMAKE_SOURCE_DIR}/test
    ${CMAKE_SOURCE_DIR}/include
)
target_compile_features(unit_tests PRIVATE cxx_std_23)
set_target_properties(unit_tests PROPERTIES CXX_EXTENSIONS OFF)

target_compile_options(unit_tests PRIVATE --coverage)
target_link_options(unit_tests PRIVATE --coverage)

target_link_libraries(
    unit_tests
    GTest::gtest_main
    range-v3::range-v3
    arrow::arrow
    reflectcpp::reflectcpp
)
target_sources(
    unit_tests
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/helpers/StringSplitter.cpp
    ${CMAKE_SOURCE_DIR}/src/io/StatementMaker.cpp
    ${CMAKE_SOURCE_DIR}/src/textmining/StringSplitter.cpp
    ${CMAKE_SOURCE_DIR}/src/textmining/Vocabulary.cpp
)

gtest_discover_tests(unit_tests)
