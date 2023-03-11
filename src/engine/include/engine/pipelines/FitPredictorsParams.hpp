// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FITPREDICTORSPARAMS_HPP_
#define ENGINE_PIPELINES_FITPREDICTORSPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <string>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "engine/pipelines/FitParams.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "fct/Ref.hpp"
#include "predictors/PredictorImpl.hpp"

namespace engine {
namespace pipelines {

struct FitPredictorsParams {
  /// A pointer to the autofeatures. This is modifiable on purpose, because we
  /// want to be able to cache generated features.
  containers::NumericalFeatures* const autofeatures_;

  /// The dependencies for the predictors (either fl_fingerprints or
  /// fs_fingerprints)
  const fct::Ref<const std::vector<commands::Fingerprint>> dependencies_;

  /// The feature learners used in this pipeline.
  const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>
      feature_learners_;

  /// The parameters passed to fit(...)
  const FitParams fit_params_;

  /// The underlying impl_
  const fct::Ref<const predictors::PredictorImpl> impl_;

  /// The peripheral tables, after applying the staging and preprocessing.
  const std::vector<containers::DataFrame> peripheral_dfs_;

  /// The underlying pipeline
  const Pipeline pipeline_;

  /// The population table, after applying the staging and preprocessing.
  const containers::DataFrame population_df_;

  /// The preprocessors used in this pipeline.
  const std::vector<fct::Ref<const preprocessors::Preprocessor>> preprocessors_;

  /// The fingerprints of the preprocessors used for fitting.
  const std::vector<commands::Fingerprint> preprocessor_fingerprints_;

  /// The purpose (feature_selector_ or predictor_)
  const std::string purpose_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPREDICTORSPARAMS_HPP_
