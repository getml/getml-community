add_library(
  communication
  OBJECT
  Logger.cpp
  Monitor.cpp
  Receiver.cpp
  Sender.cpp
  Warner.cpp
  Warnings.cpp
)

target_link_libraries(
  communication
  PUBLIC
  Poco::Net
  reflectcpp::reflectcpp
  xgboost::xgboost
)
