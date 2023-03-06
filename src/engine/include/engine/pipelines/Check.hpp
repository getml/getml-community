// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_CHECK_HPP_
#define ENGINE_PIPELINES_CHECK_HPP_

#include "commands/FeatureLearnerFingerprint.hpp"
#include "commands/PredictorFingerprint.hpp"
#include "commands/WarningFingerprint.hpp"
#include "engine/pipelines/CheckParams.hpp"
#include "engine/pipelines/Pipeline.hpp"

namespace engine {
namespace pipelines {

class Check {
 public:
  using FeatureLearnerDependencyType =
      typename commands::FeatureLearnerFingerprint::DependencyType;
  using PredictorDependencyType =
      typename commands::PredictorFingerprint::DependencyType;

 public:
  /// Checks the data model for any inconsistencies.
  static void check(const Pipeline& _pipeline, const CheckParams& _params);

 private:
  /// Generates the fingerprints for the feature learners, needed for the
  /// warning.
  static std::pair<
      std::vector<fct::Ref<featurelearners::AbstractFeatureLearner>>,
      fct::Ref<const std::vector<PredictorDependencyType>>>
  init_feature_learners(
      const Pipeline& _pipeline,
      const featurelearners::FeatureLearnerParams& _feature_learner_params,
      const CheckParams& _params);
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_CHECK_HPP_
