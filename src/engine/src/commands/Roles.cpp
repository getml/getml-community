// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/Roles.hpp"

namespace commands {

Roles Roles::from_schema(const helpers::Schema& _schema) {
  return Roles{
      .val_ = Roles::f_categorical(_schema.categoricals()) *
              Roles::f_join_key(_schema.join_keys()) *
              Roles::f_numerical(ranges::views::concat(_schema.discretes(),
                                                       _schema.numericals()) |
                                 std::ranges::to<std::vector>()) *
              Roles::f_target(_schema.targets()) *
              Roles::f_text(_schema.text()) *
              Roles::f_time_stamp(_schema.time_stamps()) *
              Roles::f_unused_float(_schema.unused_floats()) *
              Roles::f_unused_string(_schema.unused_strings())};
}

}  // namespace commands
