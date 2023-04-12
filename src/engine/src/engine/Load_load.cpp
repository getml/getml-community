// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/Load.hpp"

#include "commands/Pipeline.hpp"
#include "engine/pipelines/Fit.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "helpers/Loader.hpp"
#include "json/json.hpp"

namespace engine {
namespace pipelines {

Pipeline Load::load(
    const std::string& _path,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const std::shared_ptr<dependency::PreprocessorTracker>
        _preprocessor_tracker) {
  assert_true(_fe_tracker);
  assert_true(_pred_tracker);
  assert_true(_preprocessor_tracker);

  const auto obj =
      helpers::Loader::load_from_json<fct::Ref<const commands::Pipeline>>(
          _path + "obj.json");

  const auto scores =
      helpers::Loader::load_from_json<fct::Ref<const metrics::Scores>>(
          _path + "scores.json");

  const auto pipeline_json =
      helpers::Loader::load_from_json<PipelineJSON>(_path + "pipeline.json");

  const auto pipeline = Pipeline(obj).with_scores(scores).with_creation_time(
      pipeline_json.get<"creation_time_">());

  const auto [feature_selector_impl, predictor_impl] = load_impls(_path);

  const auto preprocessors =
      load_preprocessors(_path, _preprocessor_tracker, pipeline_json, pipeline);

  const auto feature_learners =
      load_feature_learners(_path, _fe_tracker, pipeline_json, pipeline);

  const auto feature_selectors = load_feature_selectors(
      _path, _pred_tracker, feature_selector_impl, pipeline_json, pipeline);

  const auto predictors = load_predictors(_path, _pred_tracker, predictor_impl,
                                          pipeline_json, pipeline);

  const auto fitted =
      fct::Ref<const pipelines::FittedPipeline>::make(pipelines::FittedPipeline{
          .feature_learners_ = feature_learners,
          .feature_selectors_ = feature_selectors,
          .fingerprints_ = pipeline_json,
          .modified_peripheral_schema_ =
              pipeline_json.get<"modified_peripheral_schema_">(),
          .modified_population_schema_ =
              pipeline_json.get<"modified_population_schema_">(),
          .peripheral_schema_ = pipeline_json.get<"peripheral_schema_">(),
          .population_schema_ = pipeline_json.get<"population_schema_">(),
          .predictors_ = predictors,
          .preprocessors_ = preprocessors});

  return pipeline.with_allow_http(pipeline_json.get<"allow_http_">())
      .with_fitted(fitted);
}

}  // namespace pipelines
}  // namespace engine

