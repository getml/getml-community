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
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace engine {
namespace pipelines {

using FitPredictorsParams = rfl::NamedTuple<

    /// A pointer to the autofeatures. This is modifiable on purpose, because we
    /// want to be able to cache generated features.
    rfl::Field<"autofeatures_", containers::NumericalFeatures*>,

    /// The dependencies for the predictors (either fl_fingerprints or
    /// fs_fingerprints)
    rfl::Field<"dependencies_",
               rfl::Ref<const std::vector<commands::Fingerprint>>>,

    /// The feature learners used in this pipeline.
    rfl::Field<
        "feature_learners_",
        std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>>,

    /// The parameters passed to fit(...)
    rfl::Field<"fit_params_", FitParams>,

    /// The underlying impl_
    rfl::Field<"impl_", rfl::Ref<const predictors::PredictorImpl>>,

    /// The peripheral tables, after applying the staging and preprocessing.
    rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>,

    /// The underlying pipeline
    rfl::Field<"pipeline_", Pipeline>,

    /// The population table, after applying the staging and preprocessing.
    rfl::Field<"population_df_", containers::DataFrame>,

    /// The preprocessors used in this pipeline.
    rfl::Field<"preprocessors_",
               std::vector<rfl::Ref<const preprocessors::Preprocessor>>>,

    /// The fingerprints of the preprocessors used for fitting.
    rfl::Field<"preprocessor_fingerprints_",
               rfl::Ref<const std::vector<commands::Fingerprint>>>,

    /// The purpose (feature_selector_ or predictor_)
    rfl::Field<"purpose_", Purpose>>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPREDICTORSPARAMS_HPP_
