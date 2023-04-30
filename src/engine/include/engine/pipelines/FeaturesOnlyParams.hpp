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
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace pipelines {

using FeaturesOnlyParams = fct::NamedTuple<

    /// The depedencies of the predictors.
    fct::Field<"dependencies_",
               fct::Ref<const std::vector<commands::Fingerprint>>>,

    /// The feature learners used in this pipeline.
    fct::Field<
        "feature_learners_",
        std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>>,

    /// The fingerprints of the feature selectors used for fitting.
    fct::Field<"fs_fingerprints_",
               fct::Ref<const std::vector<commands::Fingerprint>>>,

    /// The underlying pipeline
    fct::Field<"pipeline_", Pipeline>,

    /// The preprocessors used in this pipeline.
    fct::Field<"preprocessors_",
               std::vector<fct::Ref<const preprocessors::Preprocessor>>>,

    /// Pimpl for the predictors.
    fct::Field<"predictor_impl_", fct::Ref<const predictors::PredictorImpl>>,

    /// The parameters needed for transform(...).
    fct::Field<"transform_params_", TransformParams>>;

}  // namespace pipelines
}  // namespace engine
#endif  // ENGINE_PIPELINES_FEATURESONLYPARAMS_HPP_
