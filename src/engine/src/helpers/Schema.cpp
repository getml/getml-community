// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/Schema.hpp"

namespace helpers {

Schema::Schema(const SchemaImpl& _impl)
    : categoricals_(_impl.categoricals()),
      discretes_(_impl.discretes() ? *_impl.discretes()
                                   : std::vector<std::string>()),
      join_keys_(_impl.join_keys()),
      name_(_impl.name()),
      numericals_(_impl.numericals()),
      targets_(_impl.targets()),
      text_(_impl.text()),
      time_stamps_(_impl.time_stamps()),
      unused_floats_(_impl.unused_floats()),
      unused_strings_(_impl.unused_strings()) {}

// ----------------------------------------------------------------------------

typename Schema::ReflectionType Schema::reflection() const {
  return SchemaImpl{.categoricals = categoricals_,
                    .discretes = discretes_,
                    .join_keys = join_keys_,
                    .name = name_,
                    .numericals = numericals_,
                    .targets = targets_,
                    .text = text_,
                    .time_stamps = time_stamps_,
                    .unused_floats = unused_floats_,
                    .unused_strings = unused_strings_};
}

}  // namespace helpers
