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
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace commands {

using Pipeline = fct::NamedTuple<
    fct::Field<"data_model_", fct::Ref<const DataModel>>,
    fct::Field<"feature_learners_", std::vector<FeatureLearner>>,
    fct::Field<"feature_selectors_", std::vector<Predictor>>,
    fct::Field<"include_categorical_", bool>, fct::Field<"name_", std::string>,
    fct::Field<"peripheral_", fct::Ref<const std::vector<DataModel>>>,
    fct::Field<"predictors_", std::vector<Predictor>>,
    fct::Field<"preprocessors_", std::vector<Preprocessor>>,
    fct::Field<"share_selected_features_", Float>,
    fct::Field<"tags_", std::vector<std::string>>,
    fct::Field<"type_", fct::Literal<"Pipeline">>>;

}  // namespace commands

#endif  // COMMANDS_PIPELINE_HPP_
