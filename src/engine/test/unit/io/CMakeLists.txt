add_executable(test_StatementMaker test_StatementMaker.cpp)
target_link_libraries(test_StatementMaker range-v3::range-v3 io helpers textmining tsindex)

gtest_discover_tests(test_StatementMaker)
