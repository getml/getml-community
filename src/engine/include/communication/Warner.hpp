// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMUNICATION_WARNER_HPP_
#define COMMUNICATION_WARNER_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <string>
#include <vector>

#include "commands/WarningFingerprint.hpp"
#include "communication/Warning.hpp"
#include "communication/Warnings.hpp"
#include "fct/Ref.hpp"

namespace communication {

class Warner {
 public:
  Warner() : warnings_(fct::Ref<std::vector<Warning>>::make()) {}

  ~Warner() = default;

 public:
  /// Adds a new warning to the list of warnings.
  void add(const Warning& _warning) { warnings_->push_back(_warning); }

  /// Sends all warnings to the socket.
  void send(Poco::Net::StreamSocket* _socket) const;

  /// Trivial (const) getter
  const std::vector<Warning>& warnings() const { return *warnings_; }

  /// Generates a warnings object that can be used for dependency tracking.
  const fct::Ref<Warnings> to_warnings_obj(
      const commands::WarningFingerprint& _fingerprint) const {
    return fct::Ref<Warnings>::make(_fingerprint, warnings_);
  }

 private:
  /// The list of warnings to send.
  const fct::Ref<std::vector<Warning>> warnings_;
};

}  // namespace communication

#endif  // COMMUNICATION_WARNER_HPP_
