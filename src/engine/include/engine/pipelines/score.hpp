// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_SCORE_HPP_
#define ENGINE_PIPELINES_SCORE_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "containers/DataFrame.hpp"
#include "containers/NumericalFeatures.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "engine/pipelines/Predictors.hpp"
#include "metrics/Scores.hpp"

namespace engine {
namespace pipelines {
namespace score {

/// Calculates summary statistics for the features.
std::shared_ptr<const metrics::Scores> calculate_feature_stats(
    const Pipeline& _pipeline, const FittedPipeline& _fitted,
    const containers::NumericalFeatures _features,
    const containers::DataFrame& _population_df);

/// Calculates the column importances.
std::pair<std::vector<helpers::ColumnDescription>,
          std::vector<std::vector<Float>>>
column_importances(const Pipeline& _pipeline, const FittedPipeline& _fitted);

/// Calculate the feature importances.
std::vector<std::vector<Float>> feature_importances(
    const Predictors& _predictors);

/// Scores the pipeline.
fct::Ref<const metrics::Scores> score(
    const Pipeline& _pipeline, const FittedPipeline& _fitted,
    const containers::DataFrame& _population_df,
    const std::string& _population_name,
    const containers::NumericalFeatures& _yhat);

/// Expresses a nested vector in transposed form.
std::vector<std::vector<Float>> transpose(
    const std::vector<std::vector<Float>>& _original);

}  // namespace score
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_SCORE_HPP_
