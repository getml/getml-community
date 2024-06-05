// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/Fingerprint.hpp"

#include <rfl/json.hpp>

namespace commands {

Fingerprint Fingerprint::from_json(const std::string& _json_str) {
  return Fingerprint(rfl::json::read<ReflectionType>(_json_str).value());
}

Fingerprint Fingerprint::from_json_obj(const InputVarType& _json_obj) {
  static_assert(rfl::json::Reader::has_custom_constructor<Fingerprint>,
                "This should work");
  return Fingerprint(rfl::json::read<ReflectionType>(_json_obj).value());
}

std::string Fingerprint::to_json() const { return rfl::json::write(*this); }

}  // namespace commands
