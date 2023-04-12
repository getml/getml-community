// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/Fit.hpp"
#include "engine/pipelines/Load.hpp"
#include "helpers/Loader.hpp"

namespace engine {
namespace pipelines {

std::pair<fct::Ref<const predictors::PredictorImpl>,
          fct::Ref<const predictors::PredictorImpl>>
Load::load_impls(const std::string& _path) {
  const auto feature_selector_impl = helpers::Loader::load_from_json<
      fct::Ref<const predictors::PredictorImpl>>(_path +
                                                 "feature-selector-impl.json");

  const auto predictor_impl = helpers::Loader::load_from_json<
      fct::Ref<const predictors::PredictorImpl>>(_path + "predictor-impl.json");

  return std::make_pair(feature_selector_impl, predictor_impl);
}

}  // namespace pipelines
}  // namespace engine

