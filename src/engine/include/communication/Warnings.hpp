// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMUNICATION_WARNINGS_HPP_
#define COMMUNICATION_WARNINGS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <string>
#include <vector>

#include "commands/WarningFingerprint.hpp"
#include "communication/Warning.hpp"
#include "debug/debug.hpp"
#include <rfl/Ref.hpp>

namespace communication {

class Warnings {
 public:
  Warnings(const commands::WarningFingerprint& _fingerprint,
           const rfl::Ref<const std::vector<Warning>>& _warnings)
      : fingerprint_(_fingerprint), warnings_(_warnings) {}

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
