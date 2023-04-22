// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "communication/Warner.hpp"

#include "communication/Sender.hpp"
#include "json/json.hpp"

namespace communication {

void Warner::send(Poco::Net::StreamSocket* _socket) const {
  using NamedTupleType = fct::NamedTuple<
      fct::Field<"warnings_", fct::Ref<std::vector<std::string>>>>;
  const auto named_tuple =
      NamedTupleType(fct::make_field<"warnings_">(warnings_));
  Sender::send_string(json::to_json(named_tuple), _socket);
}

}  // namespace communication
