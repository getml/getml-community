// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_COMMUNICATION_WARNINGS_HPP_
#define ENGINE_COMMUNICATION_WARNINGS_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

#include "engine/JSON.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace communication {

class Warnings {
 public:
  Warnings(Poco::JSON::Object::Ptr _fingerprint,
           const std::shared_ptr<const std::vector<std::string>>& _warnings)
      : fingerprint_(_fingerprint), warnings_(_warnings) {
    assert_true(warnings_);
  }

  ~Warnings() = default;

  // ------------------------------------------------------------------------

 public:
  /// Creates a copy.
  std::shared_ptr<Warnings> clone() const {
    return std::make_shared<Warnings>(*this);
  }

  /// Returns the fingerprint of the warnings (necessary to build
  /// the dependency graphs).
  Poco::JSON::Object::Ptr fingerprint() const { return fingerprint_; }

  /// Sends all warnings to the socket.
  void send(Poco::Net::StreamSocket* _socket) const {
    Poco::JSON::Object obj;
    obj.set("warnings_", JSON::vector_to_array_ptr(*warnings_));
    const auto json_str = JSON::stringify(obj);
    Sender::send_string(json_str, _socket);
  }

  // ------------------------------------------------------------------------

 private:
  /// The fingerprint to use for the warnings.
  Poco::JSON::Object::Ptr fingerprint_;

  /// The list of warnings to send.
  const std::shared_ptr<const std::vector<std::string>> warnings_;

  // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

#endif  // ENGINE_COMMUNICATION_WARNER_HPP_
