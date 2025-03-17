add_subdirectory(config)
add_subdirectory(dependency)
add_subdirectory(handlers)
add_subdirectory(pipelines)
add_subdirectory(preprocessors)
add_subdirectory(srv)
add_subdirectory(utils)

target_sources(
  engine
  PRIVATE
  main.cpp
)
