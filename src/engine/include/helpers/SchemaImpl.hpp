// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_SCHEMAIMPL_HPP_
#define HELPERS_SCHEMAIMPL_HPP_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "rfl/Field.hpp"

namespace helpers {

struct SchemaImpl {
  rfl::Field<"categorical_", std::vector<std::string>> categoricals;
  rfl::Field<"discrete_", std::optional<std::vector<std::string>>> discretes;
  rfl::Field<"join_keys_", std::vector<std::string>> join_keys;
  rfl::Field<"name_", std::string> name;
  rfl::Field<"numerical_", std::vector<std::string>> numericals;
  rfl::Field<"targets_", std::vector<std::string>> targets;
  rfl::Field<"text_", std::vector<std::string>> text;
  rfl::Field<"time_stamps_", std::vector<std::string>> time_stamps;
  rfl::Field<"unused_floats_", std::vector<std::string>> unused_floats;
  rfl::Field<"unused_strings_", std::vector<std::string>> unused_strings;
};

}  // namespace helpers

#endif
