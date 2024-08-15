add_executable(test_ColumnView test_ColumnView.cpp)
target_link_libraries(test_ColumnView range-v3::range-v3 arrow::arrow)

gtest_discover_tests(test_ColumnView)
