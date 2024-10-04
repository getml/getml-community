// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_CONFIG_MONITOROPTIONS_
#define ENGINE_CONFIG_MONITOROPTIONS_

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <string>

namespace engine {
namespace config {

/// Configuration information for the monitor
struct MonitorOptions {
  using ReflectionType = rfl::NamedTuple<rfl::Field<"httpPort", size_t>,
                                         rfl::Field<"proxyUrl", std::string>,
                                         rfl::Field<"tcpPort", size_t> >;

 public:
  MonitorOptions(const ReflectionType& _obj)
      : http_port_(_obj.get<"httpPort">()),
        proxy_url_(_obj.get<"proxyUrl">()),
        tcp_port_(_obj.get<"tcpPort">()) {}

  MonitorOptions() : http_port_(1709), tcp_port_(1711) {}

  ~MonitorOptions() = default;

  /// Trivial accessor
  size_t http_port() const { return http_port_; }

  /// Trivial accessor.
  const std::string proxy_url() const { return proxy_url_; }

  /// Trivial accessor
  size_t tcp_port() const { return tcp_port_; }

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

  /// The HTTP port of the monitor, used for local connections.
  size_t http_port_;

  /// Any proxy server the getML monitor might be hidden behind.
  std::string proxy_url_;

  /// The port used for local connections to the monitor.
  size_t tcp_port_;
};

}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_MONITOROPTIONS_
