// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMUNICATION_WARNINGS_HPP_
#define COMMUNICATION_WARNINGS_HPP_

#include "commands/WarningFingerprint.hpp"
#include "communication/Warning.hpp"

#include <Poco/Net/StreamSocket.h>
#include <rfl/Ref.hpp>

#include <vector>

namespace communication {

class Warnings {
 public:
  Warnings(const commands::WarningFingerprint& _fingerprint,
           const rfl::Ref<const std::vector<Warning>>& _warnings);

  ~Warnings() = default;

 public:
  /// Creates a copy.
  rfl::Ref<Warnings> clone() const { return rfl::Ref<Warnings>::make(*this); }

  /// Returns the fingerprint of the warnings (necessary to build
  /// the dependency graphs).
  commands::WarningFingerprint fingerprint() const { return fingerprint_; }

  /// Sends all warnings to the socket.
  void send(Poco::Net::StreamSocket* _socket) const;

 private:
  /// The fingerprint to use for the warnings.
  const commands::WarningFingerprint fingerprint_;

  /// The list of warnings to send.
  const rfl::Ref<const std::vector<Warning>> warnings_;
};

}  // namespace communication

#endif  // COMMUNICATION_WARNER_HPP_
