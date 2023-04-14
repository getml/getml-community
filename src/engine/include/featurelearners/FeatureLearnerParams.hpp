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
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"
#include "featurelearners/Int.hpp"
#include "helpers/Placeholder.hpp"
#include "helpers/Schema.hpp"

namespace featurelearners {

using FeatureLearnerParams = fct::NamedTuple<
    fct::Field<"dependencies_",
               fct::Ref<const std::vector<commands::Fingerprint>>>,
    fct::Field<"peripheral_", fct::Ref<const std::vector<std::string>>>,
    fct::Field<"peripheral_schema_",
               fct::Ref<const std::vector<helpers::Schema>>>,
    fct::Field<"placeholder_", fct::Ref<const helpers::Placeholder>>,
    fct::Field<"population_schema_", fct::Ref<const helpers::Schema>>,
    fct::Field<"target_num_", Int>>;

}  // namespace featurelearners

#endif  // FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_

