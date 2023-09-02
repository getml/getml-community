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
#include "fct/Ref.hpp"
#include "helpers/Loader.hpp"
#include "json/json.hpp"
#include "metrics/metrics.hpp"

namespace engine {
namespace pipelines {
namespace load_fitted {

/// Loads the feature learners.
std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>
load_feature_learners(const std::string& _path,
                      const fct::Ref<dependency::FETracker> _fe_tracker,
                      const PipelineJSON& _pipeline_json,
                      const Pipeline& _pipeline);

/// Loads the feature selectors.
Predictors load_feature_selectors(
    const std::string& _path,
    const fct::Ref<dependency::PredTracker> _pred_tracker,
    const fct::Ref<const predictors::PredictorImpl>& _feature_selector_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline);

/// Loads the impls for the feature selectors and predictors.
std::pair<fct::Ref<const predictors::PredictorImpl>,
          fct::Ref<const predictors::PredictorImpl>>
load_impls(const std::string& _path);

/// Loads the predictors.
Predictors load_predictors(
    const std::string& _path,
    const fct::Ref<dependency::PredTracker> _pred_tracker,
    const fct::Ref<const predictors::PredictorImpl>& _predictor_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline);

/// Loads the preprocessors.
std::vector<fct::Ref<const preprocessors::Preprocessor>> load_preprocessors(
    const std::string& _path,
    const fct::Ref<dependency::PreprocessorTracker> _preprocessor_tracker,
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

  return _pipeline.with_fitted(fitted);
}

// -----------------------------------------------------------------------------

std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>
load_feature_learners(const std::string& _path,
                      const fct::Ref<dependency::FETracker> _fe_tracker,
                      const PipelineJSON& _pipeline_json,
                      const Pipeline& _pipeline) {
  const auto [placeholder, peripheral] = _pipeline.make_placeholder();

  const featurelearners::FeatureLearnerParams feature_learner_params =
      fct::make_field<"dependencies_">(
          _pipeline_json.get<"preprocessor_fingerprints_">()) *
      fct::make_field<"peripheral_">(peripheral) *
      fct::make_field<"peripheral_schema_">(
          _pipeline_json.get<"modified_peripheral_schema_">()) *
      fct::make_field<"placeholder_">(placeholder) *
      fct::make_field<"population_schema_">(
          _pipeline_json.get<"modified_population_schema_">()) *
      fct::make_field<"target_num_">(
          featurelearners::AbstractFeatureLearner::USE_ALL_TARGETS);

  const auto feature_learners =
      fit::init_feature_learners(_pipeline, feature_learner_params,
                                 _pipeline_json.get<"targets_">().size());

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
    const fct::Ref<dependency::PredTracker> _pred_tracker,
    const fct::Ref<const predictors::PredictorImpl>& _feature_selector_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  const auto feature_selectors = fit::init_predictors(
      _pipeline, Purpose::make<"feature_selectors_">(), _feature_selector_impl,
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
                    .predictors_ = fit::to_const(feature_selectors)};
}

// -----------------------------------------------------------------------------

std::pair<fct::Ref<const predictors::PredictorImpl>,
          fct::Ref<const predictors::PredictorImpl>>
load_impls(const std::string& _path) {
  const auto feature_selector_impl =
      helpers::Loader::load<fct::Ref<const predictors::PredictorImpl>>(
          _path + "feature-selector-impl");

  const auto predictor_impl =
      helpers::Loader::load<fct::Ref<const predictors::PredictorImpl>>(
          _path + "predictor-impl");

  return std::make_pair(feature_selector_impl, predictor_impl);
}

// -----------------------------------------------------------------------------

Predictors load_predictors(
    const std::string& _path,
    const fct::Ref<dependency::PredTracker> _pred_tracker,
    const fct::Ref<const predictors::PredictorImpl>& _predictor_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  const auto predictors = fit::init_predictors(
      _pipeline, Purpose::make<"predictors_">(), _predictor_impl,
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
                    .predictors_ = fit::to_const(predictors)};
}

// -----------------------------------------------------------------------------

std::vector<fct::Ref<const preprocessors::Preprocessor>> load_preprocessors(
    const std::string& _path,
    const fct::Ref<dependency::PreprocessorTracker> _preprocessor_tracker,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  auto preprocessors = fit::init_preprocessors(
      _pipeline, _pipeline_json.get<"df_fingerprints_">());

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

