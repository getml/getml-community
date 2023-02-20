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

// ------------------------------------------------------------------------

std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>
Load::load_feature_learners(
    const std::string& _path,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  assert_true(_fe_tracker);

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
      Fit::init_feature_learners(_pipeline, feature_learner_params,
                                 _pipeline_json.get<"targets_">().size());

  for (size_t i = 0; i < feature_learners.size(); ++i) {
    auto& fe = feature_learners.at(i);
    fe->load(_path + "feature-learner-" + std::to_string(i) + ".json");
    _fe_tracker->add(fe);
  }

  return Fit::to_const(feature_learners);
}

// ------------------------------------------------------------------------

Predictors Load::load_feature_selectors(
    const std::string& _path,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const fct::Ref<const predictors::PredictorImpl>& _feature_selector_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  const auto feature_selectors = Fit::init_predictors(
      _pipeline, "feature_selectors_", _feature_selector_impl,
      _pipeline_json.get<"fl_fingerprints_">(),
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

// ------------------------------------------------------------------------

std::pair<fct::Ref<const predictors::PredictorImpl>,
          fct::Ref<const predictors::PredictorImpl>>
Load::load_impls(const std::string& _path) {
  const auto feature_selector_impl =
      fct::Ref<const predictors::PredictorImpl>::make(
          load_json_obj(_path + "feature-selector-impl.json"));

  const auto predictor_impl = fct::Ref<predictors::PredictorImpl>::make(
      load_json_obj(_path + "predictor-impl.json"));

  return std::make_pair(feature_selector_impl, predictor_impl);
}

// ------------------------------------------------------------------------

Poco::JSON::Object Load::load_json_obj(const std::string& _fname) {
  std::ifstream input(_fname);

  std::stringstream json;

  std::string line;

  if (input.is_open()) {
    while (std::getline(input, line)) {
      json << line;
    }

    input.close();
  } else {
    throw std::runtime_error("File '" + _fname + "' not found!");
  }

  const auto ptr =
      Poco::JSON::Parser().parse(json.str()).extract<Poco::JSON::Object::Ptr>();

  if (!ptr) {
    throw std::runtime_error("JSON file did not contain an object!");
  }

  return *ptr;
}

// ----------------------------------------------------------------------------

Predictors Load::load_predictors(
    const std::string& _path,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const fct::Ref<const predictors::PredictorImpl>& _predictor_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  const auto predictors =
      Fit::init_predictors(_pipeline, "predictors_", _predictor_impl,
                           _pipeline_json.get<"fs_fingerprints_">(),
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

// ------------------------------------------------------------------------

std::vector<fct::Ref<const preprocessors::Preprocessor>>
Load::load_preprocessors(const std::string& _path,
                         const std::shared_ptr<dependency::PreprocessorTracker>
                             _preprocessor_tracker,
                         const PipelineJSON& _pipeline_json,
                         const Pipeline& _pipeline) {
  assert_true(_preprocessor_tracker);

  auto preprocessors = Fit::init_preprocessors(
      _pipeline, _pipeline_json.get<"df_fingerprints_">());

  for (size_t i = 0; i < preprocessors.size(); ++i) {
    const auto& p = preprocessors.at(i);
    p->load(_path + "preprocessor-" + std::to_string(i) + ".json");
    _preprocessor_tracker->add(p);
  }

  return Fit::to_const(preprocessors);
}

// ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine

