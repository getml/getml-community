// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FIT_HPP_
#define ENGINE_PIPELINES_FIT_HPP_

#include <Poco/Net/StreamSocket.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "commands/Predictor.hpp"
#include "containers/DataFrame.hpp"
#include "engine/pipelines/FitParams.hpp"
#include "engine/pipelines/FitPredictorsParams.hpp"
#include "engine/pipelines/FitPreprocessorsParams.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "engine/pipelines/Preprocessed.hpp"
#include "metrics/Scores.hpp"

namespace engine {
namespace pipelines {
namespace fit {

using Purpose = typename FitPredictorsParams::Purpose;

/// Extracts the fingerprints of the data frames.
fct::Ref<const std::vector<commands::Fingerprint>> extract_df_fingerprints(
    const Pipeline& _pipeline, const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs);

/// Extracts the fingerprints from the feature learners.
fct::Ref<const std::vector<commands::Fingerprint>> extract_fl_fingerprints(
    const std::vector<fct::Ref<featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const fct::Ref<const std::vector<commands::Fingerprint>>& _dependencies);

/// Extracts the fingerprints from the preprocessors.
fct::Ref<const std::vector<commands::Fingerprint>>
extract_preprocessor_fingerprints(
    const std::vector<fct::Ref<preprocessors::Preprocessor>>& _preprocessors,
    const fct::Ref<const std::vector<commands::Fingerprint>>& _dependencies);

/// Extracts schemata from the data frames
std::pair<fct::Ref<const helpers::Schema>,
          fct::Ref<const std::vector<helpers::Schema>>>
extract_schemata(const containers::DataFrame& _population_df,
                 const std::vector<containers::DataFrame>& _peripheral_dfs,
                 const bool _separate_discrete);

/// Fit the pipeline.
std::pair<fct::Ref<const FittedPipeline>, fct::Ref<const metrics::Scores>> fit(
    const Pipeline& _pipeline, const FitParams& _params);

/// Fits the pipeline up the point where we have fitted the preprocessors.
Preprocessed fit_preprocessors_only(const Pipeline& _pipeline,
                                    const FitPreprocessorsParams& _params);

/// Initializes the feature learners before fitting them.
std::vector<fct::Ref<featurelearners::AbstractFeatureLearner>>
init_feature_learners(
    const Pipeline& _pipeline,
    const featurelearners::FeatureLearnerParams& _feature_learner_params,
    const size_t _num_targets);

/// Initializes the predictors or feature selectors.
std::vector<std::vector<fct::Ref<predictors::Predictor>>> init_predictors(
    const Pipeline& _pipeline, const Purpose _purpose,
    const fct::Ref<const predictors::PredictorImpl>& _predictor_impl,
    const std::vector<commands::Fingerprint>& _dependencies,
    const size_t _num_targets);

/// Initializes the preprocessors.
std::vector<fct::Ref<preprocessors::Preprocessor>> init_preprocessors(
    const Pipeline& _pipeline,
    const fct::Ref<const std::vector<commands::Fingerprint>>& _dependencies);

/// Transforms to a vector of const references.
template <class T>
inline std::vector<fct::Ref<const T>> to_const(
    const std::vector<fct::Ref<T>>& _orig) {
  const auto as_const_ref = [](const auto& _o) {
    return fct::Ref<const T>(_o);
  };
  return fct::collect::vector<fct::Ref<const T>>(
      _orig | VIEWS::transform(as_const_ref));
}

/// Transforms to a vector of const references.
template <class T>
inline std::vector<std::vector<fct::Ref<const T>>> to_const(
    const std::vector<std::vector<fct::Ref<T>>>& _orig) {
  const auto as_const_ref = [](const auto& _o) { return fit::to_const(_o); };
  return fct::collect::vector<std::vector<fct::Ref<const T>>>(
      _orig | VIEWS::transform(as_const_ref));
}

/// Transforms to a vector of shared_ptrs to a vector of
// references.
template <class T>
inline std::vector<fct::Ref<T>> to_ref(
    const std::vector<std::shared_ptr<T>>& _orig) {
  const auto as_const_ref = [](const auto& _o) { return fct::Ref<T>(_o); };
  return fct::collect::vector<fct::Ref<T>>(_orig |
                                           VIEWS::transform(as_const_ref));
}

/// Transforms to a vector of references.
template <class T>
inline std::vector<std::vector<fct::Ref<T>>> to_ref(
    const std::vector<std::vector<std::shared_ptr<T>>>& _orig) {
  const auto as_const_ref = [](const auto& _o) { return fit::to_ref(_o); };
  return fct::collect::vector<std::vector<fct::Ref<T>>>(
      _orig | VIEWS::transform(as_const_ref));
}

}  // namespace fit
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FIT_HPP_
