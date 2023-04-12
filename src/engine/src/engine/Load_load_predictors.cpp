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

Predictors Load::load_predictors(
    const std::string& _path,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const fct::Ref<const predictors::PredictorImpl>& _predictor_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  const auto predictors =
      Fit::init_predictors(_pipeline, "predictors_", _predictor_impl,
                           *_pipeline_json.get<"fs_fingerprints_">(),
                           _pipeline_json.get<"targets_">().size());

  for (size_t i = 0; i < predictors.size(); ++i) {
    for (size_t j = 0; j < predictors.at(i).size(); ++j) {
      const auto& p = predictors.at(i).at(j);
      p->load(_path + "predictor-" + std::to_string(i) + "-" +
              std::to_string(j));
      _pred_tracker->add(p);
    }
  }

  return Predictors{.impl_ = _predictor_impl,
                    .predictors_ = Fit::to_const(predictors)};
}

}  // namespace pipelines
}  // namespace engine

