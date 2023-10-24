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
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "predictors/PredictorImpl.hpp"

namespace engine {
namespace pipelines {

using FitPredictorsParams = fct::NamedTuple<

    /// A pointer to the autofeatures. This is modifiable on purpose, because we
    /// want to be able to cache generated features.
    fct::Field<"autofeatures_", containers::NumericalFeatures*>,

    /// The dependencies for the predictors (either fl_fingerprints or
    /// fs_fingerprints)
    fct::Field<"dependencies_",
               fct::Ref<const std::vector<commands::Fingerprint>>>,

    /// The feature learners used in this pipeline.
    fct::Field<
        "feature_learners_",
        std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>>,

    /// The parameters passed to fit(...)
    fct::Field<"fit_params_", FitParams>,

    /// The underlying impl_
    fct::Field<"impl_", fct::Ref<const predictors::PredictorImpl>>,

    /// The peripheral tables, after applying the staging and preprocessing.
    fct::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>,

    /// The underlying pipeline
    fct::Field<"pipeline_", Pipeline>,

    /// The population table, after applying the staging and preprocessing.
    fct::Field<"population_df_", containers::DataFrame>,

    /// The preprocessors used in this pipeline.
    fct::Field<"preprocessors_",
               std::vector<fct::Ref<const preprocessors::Preprocessor>>>,

    /// The fingerprints of the preprocessors used for fitting.
    fct::Field<"preprocessor_fingerprints_",
               fct::Ref<const std::vector<commands::Fingerprint>>>,

    /// The purpose (feature_selector_ or predictor_)
    fct::Field<"purpose_", Purpose>>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPREDICTORSPARAMS_HPP_
