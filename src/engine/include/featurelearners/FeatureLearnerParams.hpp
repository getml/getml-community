// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_
#define FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_

#include <memory>
#include <string>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "featurelearners/Int.hpp"
#include "helpers/Placeholder.hpp"
#include "helpers/Schema.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"

namespace featurelearners {

using FeatureLearnerParams = rfl::NamedTuple<
    rfl::Field<"dependencies_",
               rfl::Ref<const std::vector<commands::Fingerprint>>>,
    rfl::Field<"peripheral_", rfl::Ref<const std::vector<std::string>>>,
    rfl::Field<"peripheral_schema_",
               rfl::Ref<const std::vector<helpers::Schema>>>,
    rfl::Field<"placeholder_", rfl::Ref<const helpers::Placeholder>>,
    rfl::Field<"population_schema_", rfl::Ref<const helpers::Schema>>,
    rfl::Field<"target_num_", Int>>;

}  // namespace featurelearners

#endif  // FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_

