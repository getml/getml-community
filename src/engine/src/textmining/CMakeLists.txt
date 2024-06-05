add_library(
  textmining
  OBJECT
  RowIndex.cpp
  StringSplitter.cpp
  Vocabulary.cpp
)

target_link_libraries(
  textmining
  PUBLIC
  reflectcpp::reflectcpp
)
