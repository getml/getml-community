// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/Load.hpp"
#include "engine/pipelines/fit.hpp"

namespace engine {
namespace pipelines {

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
      fit::init_feature_learners(_pipeline, feature_learner_params,
                                 _pipeline_json.get<"targets_">().size());

  for (size_t i = 0; i < feature_learners.size(); ++i) {
    auto& fe = feature_learners.at(i);
    fe->load(_path + "feature-learner-" + std::to_string(i) + ".json");
    _fe_tracker->add(fe);
  }

  return fit::to_const(feature_learners);
}

}  // namespace pipelines
}  // namespace engine

