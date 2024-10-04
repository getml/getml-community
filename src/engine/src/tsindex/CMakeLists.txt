add_library(
  tsindex
  OBJECT
  InMemoryIndex.cpp
)

target_link_libraries(
  tsindex
  PUBLIC
  reflectcpp::reflectcpp
)
