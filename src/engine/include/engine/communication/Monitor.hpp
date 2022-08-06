// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_COMMUNICATION_MONITOR_HPP_
#define ENGINE_COMMUNICATION_MONITOR_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <thread>

// ----------------------------------------------------------------------------

#include "engine/config/config.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace communication {
// ----------------------------------------------------------------------------

class Monitor {
  // ------------------------------------------------------------------------

 public:
  static constexpr bool TIMEOUT_ON = true;
  static constexpr bool TIMEOUT_OFF = false;

  // ------------------------------------------------------------------------

 public:
  explicit Monitor(const engine::config::Options& _options)
      : options_(_options) {
    std::thread t(shutdown_when_monitor_dies, *this);
    t.detach();
  }

  ~Monitor() {}

  // ------------------------------------------------------------------------

  /// Connects to the TCP port of the monitor.
  std::shared_ptr<Poco::Net::StreamSocket> connect(const bool _timeout) const;

  /// For logging errors
  void log(const std::string& _msg) const;

  /// Generates a command string from _type and _body.
  std::string make_cmd(
      const std::string& _type,
      const Poco::JSON::Object& _body = Poco::JSON::Object()) const;

  /// Sends a command to the TCP port of the monitor.
  std::string send_tcp(const std::string& _type,
                       const Poco::JSON::Object& _body = Poco::JSON::Object(),
                       const bool _timeout = TIMEOUT_ON) const;

  // -----------------------------------------------------------------------

 private:
  /// Regularly checks whether the monitor is still alive and shuts down the
  /// engine, if necessary.
  static void shutdown_when_monitor_dies(const Monitor _monitor);

  // -----------------------------------------------------------------------

 private:
  /// Contains information on the port of the monitor process
  const engine::config::Options options_;

  // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

#endif  // ENGINE_COMMUNICATION_MONITOR_HPP_
