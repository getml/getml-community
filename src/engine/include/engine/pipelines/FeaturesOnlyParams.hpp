// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FEATURESONLYPARAMS_HPP_
#define ENGINE_PIPELINES_FEATURESONLYPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "engine/pipelines/TransformParams.hpp"

namespace engine {
namespace pipelines {

struct FeaturesOnlyParams {
  /// The depedencies of the predictors.
  rfl::Field<"dependencies_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      dependencies;

  /// The feature learners used in this pipeline.
  rfl::Field<
      "feature_learners_",
      std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>>
      feature_learners;

  /// The fingerprints of the feature selectors used for fitting.
  rfl::Field<"fs_fingerprints_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      fs_fingerprints;

  /// The underlying pipeline
  rfl::Field<"pipeline_", Pipeline> pipeline;

  /// The preprocessors used in this pipeline.
  rfl::Field<"preprocessors_",
             std::vector<rfl::Ref<const preprocessors::Preprocessor>>>
      preprocessors;

  /// Pimpl for the predictors.
  rfl::Field<"predictor_impl_", rfl::Ref<const predictors::PredictorImpl>>
      predictor_impl;

  /// The parameters needed for transform(...).
  rfl::Field<"transform_params_", TransformParams> transform_params;
};

}  // namespace pipelines
}  // namespace engine
#endif  // ENGINE_PIPELINES_FEATURESONLYPARAMS_HPP_
