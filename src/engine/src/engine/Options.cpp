// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/config/Options.hpp"

#include <chrono>
#include <fstream>
#include <iostream>

#include "helpers/Loader.hpp"

namespace engine {
namespace config {

Options Options::make_options(int _argc, char* _argv[]) {
  auto options = parse_from_file();

  options.parse_flags(_argc, _argv);

  if (options.all_projects_directory_.back() != '/') {
    options.all_projects_directory_ += "/";
  }

  return options;
}

// ----------------------------------------------------------------------------

bool Options::parse_boolean(const std::string& _arg, const std::string& _flag,
                            bool* _target) const {
  std::string str;

  const auto success = parse_string(_arg, _flag, &str);

  if (!success) {
    return false;
  }

  if (str == "true") {
    *_target = true;
    return true;
  }

  if (str == "false") {
    *_target = false;
    return true;
  }

  return false;
}

// ----------------------------------------------------------------------------

void Options::parse_flags(int _argc, char* argv[]) {
  std::string allow_push_notifications;

  bool allow_remote_ips;

  std::string launch_browser;

  std::string log;

  std::size_t app_pid = 0;

  for (int i = 1; i < _argc; ++i) {
    const auto arg = std::string(argv[i]);

    bool success = parse_size_t(arg, "engine-port", &(engine_.port_));

    success = success || parse_boolean(arg, "in-memory", &(engine_.in_memory_));

    success = success || parse_string(arg, "project", &(engine_.project_));

    success = success || parse_size_t(arg, "http-port", &(monitor_.http_port_));

    success = success || parse_size_t(arg, "tcp-port", &(monitor_.tcp_port_));

    success = success ||
              parse_string(arg, "project-directory", &all_projects_directory_);

    success = success || parse_string(arg, "proxy-url", &(monitor_.proxy_url_));

    // This is to avoid a warning message.
    success = success || parse_string(arg, "allow-push-notifications",
                                      &allow_push_notifications);

    // This is to avoid a warning message.
    success =
        success || parse_boolean(arg, "allow-remote-ips", &allow_remote_ips);

    // This is to avoid a warning message.
    success = success || parse_string(arg, "launch-browser", &launch_browser);

    // This is to avoid a warning message.
    success = success || parse_string(arg, "log", &log);

    // This is to avoid a warning message.
    success = success || parse_size_t(arg, "app-pid", &app_pid);

    if (!success) {
      Options::print_warning("Could not parse command line flag '" + arg +
                             "'!");
    }
  }
}

// ----------------------------------------------------------------------------

Options Options::parse_from_file() {
  return helpers::Loader::load_from_json<Options>("../config.json");
}

// ----------------------------------------------------------------------------

bool Options::parse_size_t(const std::string& _arg, const std::string& _flag,
                           size_t* _target) const {
  if (_arg.find("-" + _flag + "=") != std::string::npos) {
    try {
      const auto substr = _arg.substr(_flag.size() + 2);
      const auto val = static_cast<size_t>(std::stoul(substr));
      *_target = val;
      return true;
    } catch (std::exception& e) {
      return false;
    }
  }

  return false;
}

// ----------------------------------------------------------------------------

bool Options::parse_string(const std::string& _arg, const std::string& _flag,
                           std::string* _target) const {
  if (_arg.find("-" + _flag + "=") != std::string::npos) {
    try {
      *_target = _arg.substr(_flag.size() + 2);
      return true;
    } catch (std::exception& e) {
      return false;
    }
  }

  return false;
}

// ----------------------------------------------------------------------------

void Options::print_warning(const std::string& _msg) {
  auto now = std::chrono::system_clock::now();

  std::time_t current_time = std::chrono::system_clock::to_time_t(now);

  std::cout << std::ctime(&current_time) << _msg << std::endl << std::endl;
}

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine
