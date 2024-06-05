add_library(
  transpilation
  OBJECT
  HumanReadableSQLGenerator.cpp
  HumanReadableTrimming.cpp
  SQLDialectParser.cpp
  SQLGenerator.cpp
)

target_link_libraries(
  transpilation
  PUBLIC
  reflectcpp::reflectcpp
)
