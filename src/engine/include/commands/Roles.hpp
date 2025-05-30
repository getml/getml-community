// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/Schema.hpp"

#include <range/v3/view/concat.hpp>
#include <rfl/NamedTuple.hpp>

#include <ranges>
#include <string>
#include <vector>

#ifndef COMMANDS_ROLES_HPP_
#define COMMANDS_ROLES_HPP_

namespace commands {

struct Roles {
  /// The names of the categorical columns
  using f_categorical = rfl::Field<"categorical", std::vector<std::string>>;

  /// The names of the join keys
  using f_join_key = rfl::Field<"join_key", std::vector<std::string>>;

  /// The names of the numerical columns
  using f_numerical = rfl::Field<"numerical", std::vector<std::string>>;

  /// The names of the target columns
  using f_target = rfl::Field<"target", std::vector<std::string>>;

  /// The names of the text columns
  using f_text = rfl::Field<"text", std::vector<std::string>>;

  /// The names of the time stamp columns
  using f_time_stamp = rfl::Field<"time_stamp", std::vector<std::string>>;

  /// The names of the unused float columns
  using f_unused_float = rfl::Field<"unused_float", std::vector<std::string>>;

  /// The names of the unused string columns
  using f_unused_string = rfl::Field<"unused_string", std::vector<std::string>>;

  using ReflectionType =
      rfl::NamedTuple<f_categorical, f_join_key, f_numerical, f_target, f_text,
                      f_time_stamp, f_unused_float, f_unused_string>;

  /// Retrieves the roles from the schema.
  static Roles from_schema(const helpers::Schema& _schema);

  /// Normally used for recursion, but here it is used
  /// to support the static constructors.
  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_ROLES_HPP_
