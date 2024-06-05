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
#include "engine/pipelines/Purpose.hpp"
#include "predictors/PredictorImpl.hpp"
#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>

namespace engine {
namespace pipelines {

struct FitPredictorsParams {
  /// A pointer to the autofeatures. This is modifiable on purpose, because we
  /// want to be able to cache generated features.
  rfl::Field<"autofeatures_", containers::NumericalFeatures*> autofeatures;

  /// The dependencies for the predictors (either fl_fingerprints or
  /// fs_fingerprints)
  rfl::Field<"dependencies_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      dependencies;

  /// The feature learners used in this pipeline.
  rfl::Field<
      "feature_learners_",
      std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>>
      feature_learners;

  /// The parameters passed to fit(...)
  rfl::Field<"fit_params_", FitParams> fit_params;

  /// The underlying impl_
  rfl::Field<"impl_", rfl::Ref<const predictors::PredictorImpl>> impl;

  /// The peripheral tables, after applying the staging and preprocessing.
  rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>
      peripheral_dfs;

  /// The underlying pipeline
  rfl::Field<"pipeline_", Pipeline> pipeline;

  /// The population table, after applying the staging and preprocessing.
  rfl::Field<"population_df_", containers::DataFrame> population_df;

  /// The preprocessors used in this pipeline.
  rfl::Field<"preprocessors_",
             std::vector<rfl::Ref<const preprocessors::Preprocessor>>>
      preprocessors;

  /// The fingerprints of the preprocessors used for fitting.
  rfl::Field<"preprocessor_fingerprints_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      preprocessor_fingerprints;

  /// The purpose (feature_selector_ or predictor_)
  rfl::Field<"purpose_", Purpose> purpose;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPREDICTORSPARAMS_HPP_
