// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/communication/Monitor.hpp"

// ------------------------------------------------------------------------

#include "engine/communication/Receiver.hpp"
#include "engine/communication/Sender.hpp"

// ------------------------------------------------------------------------

namespace engine {
namespace communication {
// ------------------------------------------------------------------------

std::shared_ptr<Poco::Net::StreamSocket> Monitor::connect(
    const bool _timeout) const {
  const auto host_and_port =
      "127.0.0.1:" + std::to_string(options_.monitor().tcp_port());

  const auto addr = Poco::Net::SocketAddress(host_and_port);

  const auto socket = std::make_shared<Poco::Net::StreamSocket>(addr);

  // A 30 second timeout can escape deadlocks.
  if (_timeout) {
    socket->setReceiveTimeout(Poco::Timespan(30, 0));
    socket->setSendTimeout(Poco::Timespan(30, 0));
  }

  return socket;
}

// ------------------------------------------------------------------------

void Monitor::log(const std::string& _msg) const {
  auto now = std::chrono::system_clock::now();

  std::time_t current_time = std::chrono::system_clock::to_time_t(now);

  std::cout << std::ctime(&current_time) << _msg << std::endl << std::endl;
}

// ------------------------------------------------------------------------

std::string Monitor::make_cmd(const std::string& _type,
                              const Poco::JSON::Object& _body) const {
  auto cmd = Poco::JSON::Object();

  cmd.set("body_", _body);

  cmd.set("project_", options_.engine().project_);

  cmd.set("type_", _type);

  return JSON::stringify(cmd);
}

// ------------------------------------------------------------------------

std::string Monitor::send_tcp(const std::string& _type,
                              const Poco::JSON::Object& _body,
                              const bool _timeout) const {
  try {
    const auto socket = connect(_timeout);

    const auto cmd_str = make_cmd(_type, _body);

    Sender::send_string(cmd_str, socket.get());

    return Receiver::recv_string(socket.get());
  } catch (std::exception& e) {
    return std::string("Connection with the getML monitor failed: ") + e.what();
  }
}

// ------------------------------------------------------------------------

void Monitor::shutdown_when_monitor_dies(const Monitor _monitor) {
  std::int32_t num_failed = 0;

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(3));

    const auto response = _monitor.send_tcp("isalive");

    if (response == "yes") {
      num_failed = 0;
    } else {
      ++num_failed;

      if (num_failed > 2) {
        std::cout << "Monitor seems to have to died. "
                     "Shutting down..."
                  << std::endl;
        std::exit(0);
      }
    }
  }
}

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine
