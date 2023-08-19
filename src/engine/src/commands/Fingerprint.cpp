// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/Fingerprint.hpp"

#include "json/json.hpp"

namespace commands {

Fingerprint Fingerprint::from_json(const std::string& _json_str) {
  return Fingerprint(json::from_json<NamedTupleType>(_json_str));
}

Fingerprint Fingerprint::from_json_obj(const InputVarType& _json_obj) {
  static_assert(json::has_from_json_obj_v<Fingerprint>, "This should work");
  return Fingerprint(json::from_json<NamedTupleType>(_json_obj));
}

std::string Fingerprint::to_json() const { return json::to_json(*this); }

}  // namespace commands
