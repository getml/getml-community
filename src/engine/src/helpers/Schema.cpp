// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/Schema.hpp"

namespace helpers {

Schema::Schema(const NamedTupleType& _obj)
    : categoricals_(_obj.get<f_categoricals>()),
      discretes_(_obj.get<f_discretes>() ? *_obj.get<f_discretes>()
                                         : std::vector<std::string>()),
      join_keys_(_obj.get<f_join_keys>()),
      name_(_obj.get<f_name>()),
      numericals_(_obj.get<f_numericals>()),
      targets_(_obj.get<f_targets>()),
      text_(_obj.get<f_text>()),
      time_stamps_(_obj.get<f_time_stamps>()),
      unused_floats_(_obj.get<f_unused_floats>()),
      unused_strings_(_obj.get<f_unused_strings>()) {}

// ----------------------------------------------------------------------------

Schema::~Schema() = default;

// ----------------------------------------------------------------------------

typename Schema::NamedTupleType Schema::named_tuple() const {
  return NamedTupleType(
      f_categoricals(categoricals_), f_discretes(discretes_),
      f_join_keys(join_keys_), f_name(name_), f_numericals(numericals_),
      f_targets(targets_), f_text(text_), f_time_stamps(time_stamps_),
      f_unused_floats(unused_floats_), f_unused_strings(unused_strings_));
}

}  // namespace helpers
