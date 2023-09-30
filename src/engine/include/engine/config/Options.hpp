// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_CONFIG_OPTIONS_
#define ENGINE_CONFIG_OPTIONS_

#include <string>

#include "engine/config/EngineOptions.hpp"
#include "engine/config/MonitorOptions.hpp"
#include "memmap/memmap.hpp"

namespace engine {
namespace config {

/// Configuration information for the engine
class Options {
 public:
  using ReflectionType =
      rfl::NamedTuple<rfl::Field<"projectDirectory", std::string>,
                      rfl::Field<"engine", EngineOptions>,
                      rfl::Field<"monitor", MonitorOptions>>;

 public:
  explicit Options(const ReflectionType& _obj)
      : all_projects_directory_(_obj.get<"projectDirectory">()),
        engine_(_obj.get<"engine">()),
        monitor_(_obj.get<"monitor">()) {}

  Options()
      : all_projects_directory_("../projects/"),
        engine_(EngineOptions()),
        monitor_(MonitorOptions()) {}

  ~Options() = default;

 public:
  /// Generates a new Options struct
  static Options make_options(int _argc, char* _argv[]);

 private:
  /// Parses the command line flags
  void parse_flags(int _argc, char* _argv[]);

  /// Parses a boolean flag.
  bool parse_boolean(const std::string& _arg, const std::string& _flag,
                     bool* _target) const;

  /// Parses the options from the config.json.
  static Options parse_from_file();

  /// Parses a size_t from a command line argument
  bool parse_size_t(const std::string& _arg, const std::string& _flag,
                    size_t* _target) const;

  /// Parses string from a command line argument
  bool parse_string(const std::string& _arg, const std::string& _flag,
                    std::string* _target) const;

  /// Prints a warning message that the config.json could not be parsed.
  static void print_warning(const std::string& _msg);

  // ------------------------------------------------------

 public:
  /// Trivial accessor
  const std::string& all_projects_directory() const {
    return all_projects_directory_;
  }

  /// Trivial accessor
  const EngineOptions& engine() const { return engine_; }

  /// Trivial accessor
  const MonitorOptions& monitor() const { return monitor_; }

  /// Generates a new memory-mapped pool.
  std::shared_ptr<memmap::Pool> make_pool() const {
    return engine_.in_memory_ ? std::shared_ptr<memmap::Pool>()
                              : std::make_shared<memmap::Pool>(temp_dir());
  }

  /// Generates the path for the project directory.
  std::string project_directory() const {
    return all_projects_directory() + engine().project_ + "/";
  }

  /// Generates the path for the project directory.
  std::string temp_dir() const { return project_directory() + "tmp/"; }

 private:
  /// The directory in which all projects are stored (not identical with the
  /// current project directory).
  std::string all_projects_directory_;

  /// Configurations for the engine.
  EngineOptions engine_;

  /// Configurations for the monitor.
  MonitorOptions monitor_;
};

}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_OPTIONS_
