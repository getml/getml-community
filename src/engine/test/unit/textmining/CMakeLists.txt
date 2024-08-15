add_executable(test_Vocabulary test_Vocabulary.cpp)
target_link_libraries(test_Vocabulary range-v3::range-v3 textmining)

gtest_discover_tests(test_Vocabulary)
