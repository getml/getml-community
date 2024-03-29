// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/load_fitted.hpp"

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "commands/Pipeline.hpp"
#include "engine/pipelines/Fingerprints.hpp"
#include "engine/pipelines/FitPredictorsParams.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/PipelineJSON.hpp"
#include "engine/pipelines/Predictors.hpp"
#include "engine/pipelines/fit.hpp"
#include "helpers/Loader.hpp"
#include "metrics/metrics.hpp"
#include "rfl/Ref.hpp"
#include "rfl/json.hpp"

namespace engine {
namespace pipelines {
namespace load_fitted {

/// Loads the feature learners.
std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>
load_feature_learners(const std::string& _path,
                      const rfl::Ref<dependency::FETracker> _fe_tracker,
                      const PipelineJSON& _pipeline_json,
                      const Pipeline& _pipeline);

/// Loads the feature selectors.
Predictors load_feature_selectors(
    const std::string& _path,
    const rfl::Ref<dependency::PredTracker> _pred_tracker,
    const rfl::Ref<const predictors::PredictorImpl>& _feature_selector_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline);

/// Loads the impls for the feature selectors and predictors.
std::pair<rfl::Ref<const predictors::PredictorImpl>,
          rfl::Ref<const predictors::PredictorImpl>>
load_impls(const std::string& _path);

/// Loads the predictors.
Predictors load_predictors(
    const std::string& _path,
    const rfl::Ref<dependency::PredTracker> _pred_tracker,
    const rfl::Ref<const predictors::PredictorImpl>& _predictor_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline);

/// Loads the preprocessors.
std::vector<rfl::Ref<const preprocessors::Preprocessor>> load_preprocessors(
    const std::string& _path,
    const rfl::Ref<dependency::PreprocessorTracker> _preprocessor_tracker,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline);

// -----------------------------------------------------------------------------

Pipeline load_fitted(const std::string& _path, const Pipeline& _pipeline,
                     const dependency::PipelineTrackers& _pipeline_trackers) {
  const auto pipeline_json =
      helpers::Loader::load<PipelineJSON>(_path + "pipeline");

  const auto [feature_selector_impl, predictor_impl] = load_impls(_path);

  const auto preprocessors = load_preprocessors(
      _path, _pipeline_trackers.get<"preprocessor_tracker_">(), pipeline_json,
      _pipeline);

  const auto feature_learners = load_feature_learners(
      _path, _pipeline_trackers.get<"fe_tracker_">(), pipeline_json, _pipeline);

  const auto feature_selectors =
      load_feature_selectors(_path, _pipeline_trackers.get<"pred_tracker_">(),
                             feature_selector_impl, pipeline_json, _pipeline);

  const auto predictors =
      load_predictors(_path, _pipeline_trackers.get<"pred_tracker_">(),
                      predictor_impl, pipeline_json, _pipeline);

  const auto fitted =
      rfl::Ref<const pipelines::FittedPipeline>::make(pipelines::FittedPipeline{
          .feature_learners_ = feature_learners,
          .feature_selectors_ = feature_selectors,
          .fingerprints_ = pipeline_json.fingerprints(),
          .modified_peripheral_schema_ =
              pipeline_json.modified_peripheral_schema(),
          .modified_population_schema_ =
              pipeline_json.modified_population_schema(),
          .peripheral_schema_ = pipeline_json.peripheral_schema(),
          .population_schema_ = pipeline_json.population_schema(),
          .predictors_ = predictors,
          .preprocessors_ = preprocessors});

  return _pipeline.with_fitted(fitted);
}

// -----------------------------------------------------------------------------

std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>
load_feature_learners(const std::string& _path,
                      const rfl::Ref<dependency::FETracker> _fe_tracker,
                      const PipelineJSON& _pipeline_json,
                      const Pipeline& _pipeline) {
  const auto [placeholder, peripheral] = _pipeline.make_placeholder();

  const auto feature_learner_params = featurelearners::FeatureLearnerParams{
      .dependencies = _pipeline_json.fingerprints().preprocessor_fingerprints(),
      .peripheral = peripheral,
      .peripheral_schema = _pipeline_json.modified_peripheral_schema(),
      .placeholder = placeholder,
      .population_schema = _pipeline_json.modified_population_schema(),
      .target_num = featurelearners::AbstractFeatureLearner::USE_ALL_TARGETS};

  const auto feature_learners = fit::init_feature_learners(
      _pipeline, feature_learner_params, _pipeline_json.targets().size());

  for (size_t i = 0; i < feature_learners.size(); ++i) {
    auto& fe = feature_learners.at(i);
    fe->load(_path + "feature-learner-" + std::to_string(i));
    _fe_tracker->add(fe);
  }

  return fit::to_const(feature_learners);
}

// -----------------------------------------------------------------------------

Predictors load_feature_selectors(
    const std::string& _path,
    const rfl::Ref<dependency::PredTracker> _pred_tracker,
    const rfl::Ref<const predictors::PredictorImpl>& _feature_selector_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  const auto feature_selectors = fit::init_predictors(
      _pipeline, Purpose::make<"feature_selectors_">(), _feature_selector_impl,
      *_pipeline_json.fingerprints().fl_fingerprints(),
      _pipeline_json.targets().size());

  for (size_t i = 0; i < feature_selectors.size(); ++i) {
    for (size_t j = 0; j < feature_selectors.at(i).size(); ++j) {
      const auto& p = feature_selectors.at(i).at(j);
      p->load(_path + "feature-selector-" + std::to_string(i) + "-" +
              std::to_string(j));
      _pred_tracker->add(p);
    }
  }

  return Predictors{.impl_ = _feature_selector_impl,
                    .predictors_ = fit::to_const(feature_selectors)};
}

// -----------------------------------------------------------------------------

std::pair<rfl::Ref<const predictors::PredictorImpl>,
          rfl::Ref<const predictors::PredictorImpl>>
load_impls(const std::string& _path) {
  const auto feature_selector_impl =
      helpers::Loader::load<rfl::Ref<const predictors::PredictorImpl>>(
          _path + "feature-selector-impl");

  const auto predictor_impl =
      helpers::Loader::load<rfl::Ref<const predictors::PredictorImpl>>(
          _path + "predictor-impl");

  return std::make_pair(feature_selector_impl, predictor_impl);
}

// -----------------------------------------------------------------------------

Predictors load_predictors(
    const std::string& _path,
    const rfl::Ref<dependency::PredTracker> _pred_tracker,
    const rfl::Ref<const predictors::PredictorImpl>& _predictor_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  const auto predictors = fit::init_predictors(
      _pipeline, Purpose::make<"predictors_">(), _predictor_impl,
      *_pipeline_json.fingerprints().fs_fingerprints(),
      _pipeline_json.targets().size());

  for (size_t i = 0; i < predictors.size(); ++i) {
    for (size_t j = 0; j < predictors.at(i).size(); ++j) {
      const auto& p = predictors.at(i).at(j);
      p->load(_path + "predictor-" + std::to_string(i) + "-" +
              std::to_string(j));
      _pred_tracker->add(p);
    }
  }

  return Predictors{.impl_ = _predictor_impl,
                    .predictors_ = fit::to_const(predictors)};
}

// -----------------------------------------------------------------------------

std::vector<rfl::Ref<const preprocessors::Preprocessor>> load_preprocessors(
    const std::string& _path,
    const rfl::Ref<dependency::PreprocessorTracker> _preprocessor_tracker,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  auto preprocessors = fit::init_preprocessors(
      _pipeline, _pipeline_json.fingerprints().df_fingerprints());

  for (size_t i = 0; i < preprocessors.size(); ++i) {
    const auto& p = preprocessors.at(i);
    p->load(_path + "preprocessor-" + std::to_string(i));
    _preprocessor_tracker->add(p);
  }

  return fit::to_const(preprocessors);
}

}  // namespace load_fitted
}  // namespace pipelines
}  // namespace engine

