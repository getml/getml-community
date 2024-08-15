add_library(
  io
  OBJECT
  CSVReader.cpp
  CSVWriter.cpp
  StatementMaker.cpp
)

target_link_libraries(
  io
  PUBLIC
  Poco::Foundation
  reflectcpp::reflectcpp
  range-v3::range-v3
)

