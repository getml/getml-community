// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMUNICATION_WARNINGS_HPP_
#define ENGINE_COMMUNICATION_WARNINGS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <string>
#include <vector>

#include "commands/WarningFingerprint.hpp"
#include "debug/debug.hpp"
#include "fct/Ref.hpp"
#include "json/json.hpp"

namespace engine {
namespace communication {

class Warnings {
 public:
  Warnings(const commands::WarningFingerprint& _fingerprint,
           const fct::Ref<const std::vector<std::string>>& _warnings)
      : fingerprint_(_fingerprint), warnings_(_warnings) {}

  ~Warnings() = default;

 public:
  /// Creates a copy.
  fct::Ref<Warnings> clone() const { return fct::Ref<Warnings>::make(*this); }

  /// Returns the fingerprint of the warnings (necessary to build
  /// the dependency graphs).
  commands::WarningFingerprint fingerprint() const { return fingerprint_; }

  /// Sends all warnings to the socket.
  void send(Poco::Net::StreamSocket* _socket) const {
    using NamedTupleType = fct::NamedTuple<
        fct::Field<"warnings_", fct::Ref<const std::vector<std::string>>>>;
    const auto named_tuple =
        NamedTupleType(fct::make_field<"warnings_">(warnings_));
    Sender::send_string(json::to_json(named_tuple), _socket);
  }

 private:
  /// The fingerprint to use for the warnings.
  const commands::WarningFingerprint fingerprint_;

  /// The list of warnings to send.
  const fct::Ref<const std::vector<std::string>> warnings_;
};

}  // namespace communication
}  // namespace engine

#endif  // ENGINE_COMMUNICATION_WARNER_HPP_
