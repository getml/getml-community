add_library(
  memmap
  OBJECT
  FreeBlocksTracker.cpp
  Pool.cpp
)

target_link_libraries(
  memmap
  PUBLIC
  Poco::Foundation
)
