#ifndef ENGINE_PIPELINES_TRANSFORM_HPP_
#define ENGINE_PIPELINES_TRANSFORM_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

// ----------------------------------------------------------------------------

#include <algorithm>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "engine/communication/communication.hpp"
#include "engine/containers/DataFrame.hpp"
#include "engine/containers/containers.hpp"
#include "engine/dependency/dependency.hpp"

// ----------------------------------------------------------------------------

#include "engine/pipelines/FeaturesOnlyParams.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "engine/pipelines/TransformParams.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

class Transform {
 public:
  /// Generates the predictions using the predictors.
  static containers::NumericalFeatures generate_predictions(
      const FittedPipeline& _fitted,
      const containers::CategoricalFeatures& _categorical_features,
      const containers::NumericalFeatures& _numerical_features);

  /// Gets the categorical features from _population_df.
  static containers::CategoricalFeatures get_categorical_features(
      const Pipeline& _pipeline, const Poco::JSON::Object& _cmd,
      const containers::DataFrame& _population_df,
      const predictors::PredictorImpl& _predictor_impl);

  /// Makes all of the features, both automatic and manual.
  static std::tuple<containers::NumericalFeatures,
                    containers::CategoricalFeatures,
                    containers::NumericalFeatures>
  make_features(const TransformParams& _params, const Pipeline& _pipeline,
                const std::vector<
                    fct::Ref<const featurelearners::AbstractFeatureLearner>>&
                    _feature_learners,
                const predictors::PredictorImpl& _predictor_impl,
                const std::vector<Poco::JSON::Object::Ptr>& _fs_fingerprints);

  /// Applies the staging step.
  static std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  stage_data_frames(const Pipeline& _pipeline,
                    const containers::DataFrame& _population_df,
                    const std::vector<containers::DataFrame>& _peripheral_dfs,
                    const std::shared_ptr<const communication::Logger>& _logger,
                    const std::optional<std::string>& _temp_dir,
                    Poco::Net::StreamSocket* _socket);

  /// Transforms a set of input data using the fitted pipeline.
  static std::tuple<containers::NumericalFeatures,
                    containers::CategoricalFeatures,
                    std::shared_ptr<const metrics::Scores>>
  transform(const TransformParams& _params, const Pipeline& _pipeline,
            const FittedPipeline& _fitted);

  /// Conducts all of the transform steps up to make_features(...), including
  /// staging and preprocessing.
  static std::tuple<containers::NumericalFeatures,
                    containers::CategoricalFeatures, containers::DataFrame>
  transform_features_only(const FeaturesOnlyParams& _params);

 private:
  /// Applies the preprocessors.
  static std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  apply_preprocessors(
      const FeaturesOnlyParams& _params,
      const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs);

  /// Calculates statistics about the features to be displayed
  /// in the frontend.
  static std::optional<metrics::Scores> calculate_feature_stats(
      const Pipeline& _pipeline, const containers::NumericalFeatures _features,
      const size_t _ncols, const Poco::JSON::Object& _cmd,
      const containers::DataFrame& _population_df);

  /// Gets the numerical columns from _population_df and
  /// returns a combination of the autofeatures and the
  /// numerical columns.
  static containers::NumericalFeatures get_numerical_features(
      const containers::NumericalFeatures& _autofeatures,
      const Poco::JSON::Object& _cmd,
      const containers::DataFrame& _population_df,
      const predictors::PredictorImpl& _predictor_impl);

  /// Generates the autofeatures.
  static containers::NumericalFeatures generate_autofeatures(
      const TransformParams& _params,
      const std::vector<
          fct::Ref<const featurelearners::AbstractFeatureLearner>>&
          _feature_learners,
      const predictors::PredictorImpl& _predictor_impl);

  /// Makes or retrieves the autofeatures as part of make_features(...).
  static containers::NumericalFeatures make_autofeatures(
      const TransformParams& _params,
      const std::vector<
          fct::Ref<const featurelearners::AbstractFeatureLearner>>&
          _feature_learners,
      const predictors::PredictorImpl& _predictor_impl);

  /// Retrieves the features from a cached data frame.
  static std::tuple<containers::NumericalFeatures,
                    containers::CategoricalFeatures,
                    containers::NumericalFeatures>
  retrieve_features_from_cache(const containers::DataFrame& _df);

  /// Selects the autofeatures based on the feature selectors.
  static containers::NumericalFeatures select_autofeatures(
      const containers::NumericalFeatures& _autofeatures,
      const std::vector<
          fct::Ref<const featurelearners::AbstractFeatureLearner>>&
          _feature_learners,
      const predictors::PredictorImpl& _predictor_impl);
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TRANSFORM_HPP_
