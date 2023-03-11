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
#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "engine/pipelines/FitParams.hpp"
#include "engine/pipelines/FitPredictorsParams.hpp"
#include "engine/pipelines/FitPreprocessorsParams.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/MakeFeaturesParams.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "engine/pipelines/Preprocessed.hpp"
#include "metrics/metrics.hpp"

namespace engine {
namespace pipelines {

class Fit {
 public:
  /// Extracts the fingerprints of the data frames.
  static fct::Ref<const std::vector<commands::Fingerprint>>
  extract_df_fingerprints(
      const Pipeline& _pipeline, const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs);

  /// Extracts the fingerprints from the feature learners.
  static fct::Ref<const std::vector<commands::Fingerprint>>
  extract_fl_fingerprints(
      const std::vector<fct::Ref<featurelearners::AbstractFeatureLearner>>&
          _feature_learners,
      const fct::Ref<const std::vector<commands::Fingerprint>>& _dependencies);

  /// Extracts the fingerprints from the preprocessors.
  static fct::Ref<const std::vector<commands::Fingerprint>>
  extract_preprocessor_fingerprints(
      const std::vector<fct::Ref<preprocessors::Preprocessor>>& _preprocessors,
      const fct::Ref<const std::vector<commands::Fingerprint>>& _dependencies);

  /// Extracts schemata from the data frames
  static std::pair<fct::Ref<const helpers::Schema>,
                   fct::Ref<const std::vector<helpers::Schema>>>
  extract_schemata(const containers::DataFrame& _population_df,
                   const std::vector<containers::DataFrame>& _peripheral_dfs,
                   const bool _separate_discrete);

  /// Fit the pipeline.
  static std::pair<fct::Ref<const FittedPipeline>,
                   fct::Ref<const metrics::Scores>>
  fit(const Pipeline& _pipeline, const FitParams& _params);

  /// Fits the pipeline up the point where we have fitted the preprocessors.
  static Preprocessed fit_preprocessors_only(
      const Pipeline& _pipeline, const FitPreprocessorsParams& _params);

  /// Initializes the feature learners before fitting them.
  static std::vector<fct::Ref<featurelearners::AbstractFeatureLearner>>
  init_feature_learners(
      const Pipeline& _pipeline,
      const featurelearners::FeatureLearnerParams& _feature_learner_params,
      const size_t _num_targets);

  /// Initializes the predictors or feature selectors.
  static std::vector<std::vector<fct::Ref<predictors::Predictor>>>
  init_predictors(
      const Pipeline& _pipeline, const std::string& _elem,
      const fct::Ref<const predictors::PredictorImpl>& _predictor_impl,
      const std::vector<commands::Fingerprint>& _dependencies,
      const size_t _num_targets);

  /// Initializes the preprocessors.
  static std::vector<fct::Ref<preprocessors::Preprocessor>> init_preprocessors(
      const Pipeline& _pipeline,
      const fct::Ref<const std::vector<commands::Fingerprint>>& _dependencies);

 public:
  /// Transforms to a vector of const references.
  template <class T>
  static std::vector<fct::Ref<const T>> to_const(
      const std::vector<fct::Ref<T>>& _orig) {
    const auto as_const_ref = [](const auto& _o) {
      return fct::Ref<const T>(_o);
    };
    return fct::collect::vector<fct::Ref<const T>>(
        _orig | VIEWS::transform(as_const_ref));
  }

  /// Transforms to a vector of const references.
  template <class T>
  static std::vector<std::vector<fct::Ref<const T>>> to_const(
      const std::vector<std::vector<fct::Ref<T>>>& _orig) {
    const auto as_const_ref = [](const auto& _o) { return Fit::to_const(_o); };
    return fct::collect::vector<std::vector<fct::Ref<const T>>>(
        _orig | VIEWS::transform(as_const_ref));
  }

  /// Transforms to a vector of shared_ptrs to a vector of
  // references.
  template <class T>
  static std::vector<fct::Ref<T>> to_ref(
      const std::vector<std::shared_ptr<T>>& _orig) {
    const auto as_const_ref = [](const auto& _o) { return fct::Ref<T>(_o); };
    return fct::collect::vector<fct::Ref<T>>(_orig |
                                             VIEWS::transform(as_const_ref));
  }

  /// Transforms to a vector of references.
  template <class T>
  static std::vector<std::vector<fct::Ref<T>>> to_ref(
      const std::vector<std::vector<std::shared_ptr<T>>>& _orig) {
    const auto as_const_ref = [](const auto& _o) { return Fit::to_ref(_o); };
    return fct::collect::vector<std::vector<fct::Ref<T>>>(
        _orig | VIEWS::transform(as_const_ref));
  }

 private:
  /// Prints the purpose in a more readable format.
  static std::string beautify_purpose(const std::string& _purpose);

  /// Calculates an index ranking the features by importance.
  static std::vector<size_t> calculate_importance_index(
      const Predictors& _feature_selectors);

  /// Calculates the sum of importances over all indices.
  static std::vector<Float> calculate_sum_importances(
      const Predictors& _feature_selectors);

  /// Extracts the fingerprints from the predictors.
  static fct::Ref<const std::vector<commands::Fingerprint>>
  extract_predictor_fingerprints(
      const std::vector<std::vector<fct::Ref<predictors::Predictor>>>&
          _predictors,
      const fct::Ref<const std::vector<commands::Fingerprint>>& _dependencies);

  /// Fits the feature learners. Returns the fitted feature learners and their
  /// fingerprints.
  static std::pair<
      std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>,
      fct::Ref<const std::vector<commands::Fingerprint>>>
  fit_feature_learners(
      const Pipeline& _pipeline, const FitParams& _params,
      const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs,
      const featurelearners::FeatureLearnerParams& _feature_learner_params);

  /// Fits the predictors. Returns the fitted predictors and their
  /// fingerprints.
  static std::pair<Predictors,
                   fct::Ref<const std::vector<commands::Fingerprint>>>
  fit_predictors(const FitPredictorsParams& _params);

  /// Fits the preprocessors and applies them to the training set.
  static std::pair<std::vector<fct::Ref<const preprocessors::Preprocessor>>,
                   fct::Ref<const std::vector<commands::Fingerprint>>>
  fit_transform_preprocessors(
      const Pipeline& _pipeline, const FitPreprocessorsParams& _params,
      const fct::Ref<const std::vector<commands::Fingerprint>>& _dependencies,
      containers::DataFrame* _population_df,
      std::vector<containers::DataFrame>* _peripheral_dfs);

  /// Retrieves the targets from the population dataframe.
  static std::vector<std::string> get_targets(
      const containers::DataFrame& _population_df);

  /// Generates the impl for the feature selectors.
  static fct::Ref<const predictors::PredictorImpl> make_feature_selector_impl(
      const Pipeline& _pipeline,
      const std::vector<
          fct::Ref<const featurelearners::AbstractFeatureLearner>>&
          _feature_learners,
      const containers::DataFrame& _population_df);

  /// Generates the features for the validation.
  static std::pair<std::optional<containers::NumericalFeatures>,
                   std::optional<containers::CategoricalFeatures>>
  make_features_validation(const FitPredictorsParams& _params);

  /// Generates the impl for the predictors.
  static fct::Ref<const predictors::PredictorImpl> make_predictor_impl(
      const Pipeline& _pipeline, const Predictors& _feature_selectors,
      const containers::DataFrame& _population_df);

  /// Generates the metrics::Scores object which is also returned by fit.
  static fct::Ref<const metrics::Scores> make_scores(
      const std::optional<MakeFeaturesParams>& _score_params,
      const Pipeline& _pipeline, const FittedPipeline& _fitted);

  /// Retrieves the predictors from the tracker.
  static std::pair<
      std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>, bool>
  retrieve_predictors(
      const fct::Ref<dependency::PredTracker>& _pred_tracker,
      const std::vector<std::vector<fct::Ref<predictors::Predictor>>>&
          _predictors);

  /// Scores the pipeline in-sample after it has been successfully fitted.
  static fct::Ref<const metrics::Scores> score_after_fitting(
      const MakeFeaturesParams& _params, const Pipeline& _pipeline,
      const FittedPipeline& _fitted);
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FIT_HPP_
