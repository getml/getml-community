// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "communication/Warnings.hpp"

#include "communication/Sender.hpp"

#include <rfl/json/write.hpp>
#include <rfl/make_named_tuple.hpp>

namespace communication {

Warnings::Warnings(const commands::WarningFingerprint& _fingerprint,
                   const rfl::Ref<const std::vector<Warning>>& _warnings)
    : fingerprint_(_fingerprint), warnings_(_warnings) {}

void Warnings::send(Poco::Net::StreamSocket* _socket) const {
  const auto named_tuple =
      rfl::make_named_tuple(rfl::make_field<"warnings_">(warnings_));
  Sender::send_string(rfl::json::write(named_tuple), _socket);
}
}  // namespace communication
