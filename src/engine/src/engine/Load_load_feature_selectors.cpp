// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/Fit.hpp"
#include "engine/pipelines/Load.hpp"

namespace engine {
namespace pipelines {

Predictors Load::load_feature_selectors(
    const std::string& _path,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const fct::Ref<const predictors::PredictorImpl>& _feature_selector_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  const auto feature_selectors = Fit::init_predictors(
      _pipeline, "feature_selectors_", _feature_selector_impl,
      *_pipeline_json.get<"fl_fingerprints_">(),
      _pipeline_json.get<"targets_">().size());

  for (size_t i = 0; i < feature_selectors.size(); ++i) {
    for (size_t j = 0; j < feature_selectors.at(i).size(); ++j) {
      const auto& p = feature_selectors.at(i).at(j);
      p->load(_path + "feature-selector-" + std::to_string(i) + "-" +
              std::to_string(j));
      _pred_tracker->add(p);
    }
  }

  return Predictors{.impl_ = _feature_selector_impl,
                    .predictors_ = Fit::to_const(feature_selectors)};
}

}  // namespace pipelines
}  // namespace engine

