// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FEATURESONLYPARAMS_HPP_
#define ENGINE_PIPELINES_FEATURESONLYPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <optional>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "engine/pipelines/TransformParams.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace pipelines {

struct FeaturesOnlyParams {
  /// The depedencies of the predictors.
  const fct::Ref<const std::vector<commands::Fingerprint>> dependencies_;

  /// The feature learners used in this pipeline.
  const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>
      feature_learners_;

  /// The fingerprints of the feature selectors used for fitting.
  const fct::Ref<const std::vector<commands::Fingerprint>> fs_fingerprints_;

  /// The underlying pipeline
  const Pipeline pipeline_;

  /// The preprocessors used in this pipeline.
  const std::vector<fct::Ref<const preprocessors::Preprocessor>> preprocessors_;

  /// Pimpl for the predictors.
  const fct::Ref<const predictors::PredictorImpl> predictor_impl_;

  /// The parameters needed for transform(...).
  const TransformParams transform_params_;
};

}  // namespace pipelines
}  // namespace engine
#endif  // ENGINE_PIPELINES_FEATURESONLYPARAMS_HPP_
