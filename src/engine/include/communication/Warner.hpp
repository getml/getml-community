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
#include "communication/Sender.hpp"
#include "communication/Warnings.hpp"
#include "fct/Ref.hpp"
#include "json/json.hpp"

namespace communication {

class Warner {
 public:
  Warner() : warnings_(fct::Ref<std::vector<std::string>>::make()) {}

  ~Warner() = default;

 public:
  /// Adds a new warning to the list of warnings.
  void add(const std::string& _warning) { warnings_->push_back(_warning); }

  /// Sends all warnings to the socket.
  void send(Poco::Net::StreamSocket* _socket) const {
    using NamedTupleType = fct::NamedTuple<
        fct::Field<"warnings_", fct::Ref<std::vector<std::string>>>>;
    const auto named_tuple =
        NamedTupleType(fct::make_field<"warnings_">(warnings_));
    Sender::send_string(json::to_json(named_tuple), _socket);
  }

  /// Trivial (const) getter
  const std::vector<std::string>& warnings() const { return *warnings_; }

  /// Generates a warnings object that can be used for dependency tracking.
  const fct::Ref<Warnings> to_warnings_obj(
      const commands::WarningFingerprint& _fingerprint) const {
    return fct::Ref<Warnings>::make(_fingerprint, warnings_);
  }

 private:
  /// The list of warnings to send.
  const fct::Ref<std::vector<std::string>> warnings_;
};

}  // namespace communication

#endif  // COMMUNICATION_WARNER_HPP_
