// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/config/MonitorOptions.hpp"

namespace engine::config {

MonitorOptions::MonitorOptions(const ReflectionType& _obj)
    : http_port_(_obj.get<"httpPort">()),
      proxy_url_(_obj.get<"proxyUrl">()),
      tcp_port_(_obj.get<"tcpPort">()) {}

MonitorOptions::MonitorOptions() : http_port_(1709), tcp_port_(1711) {}

}  // namespace engine::config
