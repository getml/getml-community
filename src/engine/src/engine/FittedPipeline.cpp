// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/pipelines/FittedPipeline.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

// ----------------------------------------------------------------------------

bool FittedPipeline::is_classification() const {
  const auto is_cl = [](const auto& elem) { return elem->is_classification(); };

  bool all_classifiers =
      std::all_of(feature_learners_.begin(), feature_learners_.end(), is_cl);

  for (const auto& fs : feature_selectors_.predictors_) {
    all_classifiers =
        all_classifiers && std::all_of(fs.begin(), fs.end(), is_cl);
  }

  for (const auto& p : predictors_.predictors_) {
    all_classifiers = all_classifiers && std::all_of(p.begin(), p.end(), is_cl);
  }

  bool all_regressors =
      std::none_of(feature_learners_.begin(), feature_learners_.end(), is_cl);

  for (const auto& fs : feature_selectors_.predictors_) {
    all_regressors =
        all_regressors && std::none_of(fs.begin(), fs.end(), is_cl);
  }

  for (const auto& p : predictors_.predictors_) {
    all_regressors = all_regressors && std::none_of(p.begin(), p.end(), is_cl);
  }

  if (!all_classifiers && !all_regressors) {
    throw std::runtime_error(
        "You are mixing classification and regression algorithms. "
        "Please make sure that all of your feature learners, feature "
        "selectors and predictors are either all regression algorithms "
        "or all classifications algorithms.");
  }

  if (all_classifiers == all_regressors) {
    throw std::runtime_error(
        "The pipelines needs at least one feature learner, feature "
        "selector or predictor.");
  }

  return all_classifiers;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
