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
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace engine {
namespace pipelines {

using FeaturesOnlyParams = rfl::NamedTuple<

    /// The depedencies of the predictors.
    rfl::Field<"dependencies_",
               rfl::Ref<const std::vector<commands::Fingerprint>>>,

    /// The feature learners used in this pipeline.
    rfl::Field<
        "feature_learners_",
        std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>>,

    /// The fingerprints of the feature selectors used for fitting.
    rfl::Field<"fs_fingerprints_",
               rfl::Ref<const std::vector<commands::Fingerprint>>>,

    /// The underlying pipeline
    rfl::Field<"pipeline_", Pipeline>,

    /// The preprocessors used in this pipeline.
    rfl::Field<"preprocessors_",
               std::vector<rfl::Ref<const preprocessors::Preprocessor>>>,

    /// Pimpl for the predictors.
    rfl::Field<"predictor_impl_", rfl::Ref<const predictors::PredictorImpl>>,

    /// The parameters needed for transform(...).
    rfl::Field<"transform_params_", TransformParams>>;

}  // namespace pipelines
}  // namespace engine
#endif  // ENGINE_PIPELINES_FEATURESONLYPARAMS_HPP_
