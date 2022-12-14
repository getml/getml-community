// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_PIPELINES_CHECK_HPP_
#define ENGINE_PIPELINES_CHECK_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include "engine/pipelines/CheckParams.hpp"
#include "engine/pipelines/Pipeline.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

class Check {
 public:
  /// Checks the data model for any inconsistencies.
  static void check(const Pipeline& _pipeline, const CheckParams& _params);

 private:
  /// Generates the fingerprints for the feature learners, needed for the
  /// warning.
  static std::pair<
      std::vector<fct::Ref<featurelearners::AbstractFeatureLearner>>,
      std::vector<Poco::JSON::Object::Ptr>>
  init_feature_learners(
      const Pipeline& _pipeline,
      const featurelearners::FeatureLearnerParams& _feature_learner_params,
      const CheckParams& _params);

  /// Generates the fingerprints for the warning.
  static Poco::JSON::Object::Ptr make_warning_fingerprint(
      const std::vector<Poco::JSON::Object::Ptr>& _fl_fingerprints);
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_CHECK_HPP_
