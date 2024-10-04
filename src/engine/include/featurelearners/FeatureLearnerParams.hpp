// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_
#define FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <rfl/TaggedUnion.hpp>
#include <string>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "featurelearners/Int.hpp"
#include "helpers/Placeholder.hpp"
#include "helpers/Schema.hpp"

namespace featurelearners {

struct FeatureLearnerParams {
  rfl::Field<"dependencies_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      dependencies;
  rfl::Field<"peripheral_", rfl::Ref<const std::vector<std::string>>>
      peripheral;
  rfl::Field<"peripheral_schema_", rfl::Ref<const std::vector<helpers::Schema>>>
      peripheral_schema;
  rfl::Field<"placeholder_", rfl::Ref<const helpers::Placeholder>> placeholder;
  rfl::Field<"population_schema_", rfl::Ref<const helpers::Schema>>
      population_schema;
  rfl::Field<"target_num_", Int> target_num;
};

}  // namespace featurelearners

#endif  // FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_
