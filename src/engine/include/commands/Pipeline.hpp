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
#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>

namespace commands {

struct Pipeline {
  rfl::Field<"data_model_", rfl::Ref<const DataModel>> data_model;
  rfl::Field<"feature_learners_", std::vector<FeatureLearner>> feature_learners;
  rfl::Field<"feature_selectors_", std::vector<Predictor>> feature_selectors;
  rfl::Field<"include_categorical_", bool> include_categorical;
  rfl::Field<"name_", std::string> name;
  rfl::Field<"peripheral_", rfl::Ref<const std::vector<DataModel>>> peripheral;
  rfl::Field<"predictors_", std::vector<Predictor>> predictors;
  rfl::Field<"preprocessors_", std::vector<Preprocessor>> preprocessors;
  rfl::Field<"share_selected_features_", Float> share_selected_features;
  rfl::Field<"tags_", std::vector<std::string>> tags;
  rfl::Field<"type_", rfl::Literal<"Pipeline">> type;
};

}  // namespace commands

#endif  // COMMANDS_PIPELINE_HPP_
