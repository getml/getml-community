// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_CONFIG_MONITOROPTIONS_
#define ENGINE_CONFIG_MONITOROPTIONS_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <string>

// ----------------------------------------------------------------------------

#include "engine/JSON.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace config {
// ----------------------------------------------------------------------------
// Configuration information for the monitor

struct MonitorOptions {
  // ------------------------------------------------------

 public:
  MonitorOptions(const Poco::JSON::Object& _json_obj)
      : http_port_(JSON::get_value<size_t>(_json_obj, "httpPort")),
        https_port_(JSON::get_value<size_t>(_json_obj, "httpsPort")),
        proxy_url_(JSON::get_value<std::string>(_json_obj, "proxyUrl")),
        tcp_port_(JSON::get_value<size_t>(_json_obj, "tcpPort")) {}

  MonitorOptions() : http_port_(1709), https_port_(1710), tcp_port_(1711) {}

  ~MonitorOptions() = default;

  // ------------------------------------------------------

  /// Trivial accessor
  const size_t http_port() const { return http_port_; }

  /// Trivial accessor
  const size_t https_port() const { return https_port_; }

  /// Trivial accessor.
  const std::string proxy_url() const { return proxy_url_; }

  /// Trivial accessor
  const size_t tcp_port() const { return tcp_port_; }

  /// Returns a URL under which the monitor can be reached.
  const std::string url() const {
    if (proxy_url_ == "") {
      return "http://localhost:" + std::to_string(http_port()) + "/#/";
    }

    if (proxy_url_.back() == '/') {
      return proxy_url_ + "#/";
    }

    return proxy_url_ + "/#/";
  }
  // ------------------------------------------------------

  /// The HTTP port of the monitor, used for local connections.
  size_t http_port_;

  /// The port of the monitor that also accepts
  /// remote connections. (The engine will never communicate
  /// with this port. It is only needed to print out the initial message).
  size_t https_port_;

  /// Any proxy server the getML monitor might be hidden behind.
  std::string proxy_url_;

  /// The port used for local connections to the monitor.
  size_t tcp_port_;

  // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_MONITOROPTIONS_
