// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_TRANSFORM_HPP_
#define ENGINE_PIPELINES_TRANSFORM_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "containers/CategoricalFeatures.hpp"
#include "containers/DataFrame.hpp"
#include "engine/pipelines/FeaturesOnlyParams.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/MakeFeaturesParams.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "engine/pipelines/TransformParams.hpp"

namespace engine {
namespace pipelines {
namespace transform {

/// Generates the predictions using the predictors.
containers::NumericalFeatures generate_predictions(
    const FittedPipeline& _fitted,
    const containers::CategoricalFeatures& _categorical_features,
    const containers::NumericalFeatures& _numerical_features);

/// Gets the categorical features from _population_df.
containers::CategoricalFeatures get_categorical_features(
    const Pipeline& _pipeline, const containers::DataFrame& _population_df,
    const predictors::PredictorImpl& _predictor_impl);

/// Makes all of the features, both automatic and manual.
std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           containers::NumericalFeatures>
make_features(
    const MakeFeaturesParams& _params, const Pipeline& _pipeline,
    const std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl,
    const std::vector<commands::Fingerprint>& _fs_fingerprints);

/// Applies the staging step.
std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
stage_data_frames(const Pipeline& _pipeline,
                  const containers::DataFrame& _population_df,
                  const std::vector<containers::DataFrame>& _peripheral_dfs,
                  const std::shared_ptr<const communication::Logger>& _logger,
                  const std::optional<std::string>& _temp_dir,
                  Poco::Net::StreamSocket* _socket);

/// Transforms a set of input data using the fitted pipeline.
std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           std::shared_ptr<const metrics::Scores>>
transform(const TransformParams& _params, const Pipeline& _pipeline,
          const FittedPipeline& _fitted);

/// Conducts all of the transform steps up to make_features(...), including
/// staging and preprocessing.
std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           containers::DataFrame>
transform_features_only(const FeaturesOnlyParams& _params);

}  // namespace transform
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TRANSFORM_HPP_
