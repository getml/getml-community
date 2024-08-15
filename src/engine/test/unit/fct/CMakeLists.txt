add_executable(test_to test_to.cpp)
target_link_libraries(test_to range-v3::range-v3)

gtest_discover_tests(test_to)
