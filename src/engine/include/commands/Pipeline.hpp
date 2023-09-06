// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_PIPELINE_HPP_
#define COMMANDS_PIPELINE_HPP_

#include <string>
#include <vector>

#include "commands/DataModel.hpp"
#include "commands/FeatureLearner.hpp"
#include "commands/Float.hpp"
#include "commands/Predictor.hpp"
#include "commands/Preprocessor.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace commands {

using Pipeline = rfl::NamedTuple<
    rfl::Field<"data_model_", rfl::Ref<const DataModel>>,
    rfl::Field<"feature_learners_", std::vector<FeatureLearner>>,
    rfl::Field<"feature_selectors_", std::vector<Predictor>>,
    rfl::Field<"include_categorical_", bool>, rfl::Field<"name_", std::string>,
    rfl::Field<"peripheral_", rfl::Ref<const std::vector<DataModel>>>,
    rfl::Field<"predictors_", std::vector<Predictor>>,
    rfl::Field<"preprocessors_", std::vector<Preprocessor>>,
    rfl::Field<"share_selected_features_", Float>,
    rfl::Field<"tags_", std::vector<std::string>>,
    rfl::Field<"type_", rfl::Literal<"Pipeline">>>;

}  // namespace commands

#endif  // COMMANDS_PIPELINE_HPP_
