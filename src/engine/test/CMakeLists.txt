add_subdirectory(unit)

set(
    TEST_TARGETS
    test_ColumnView
    test_StatementMaker
    test_Vocabulary
)
foreach(target IN ITEMS ${TEST_TARGETS})

  target_link_libraries(${target} GTest::gtest_main)
  target_include_directories(${target} PRIVATE ${CMAKE_SOURCE_DIR}/test
                                               ${CMAKE_SOURCE_DIR}/include)
  target_compile_features(${target} PRIVATE cxx_std_23)
  set_target_properties(${target} PROPERTIES CXX_EXTENSIONS OFF)

  target_compile_options(${target} PRIVATE --coverage)
  target_link_options(${target} PRIVATE --coverage)
endforeach()
