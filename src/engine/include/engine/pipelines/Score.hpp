#ifndef ENGINE_PIPELINES_SCORE_HPP_
#define ENGINE_PIPELINES_SCORE_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

// ----------------------------------------------------------------------------

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "metrics/metrics.hpp"

// ----------------------------------------------------------------------------

#include "engine/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/Pipeline.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

class Score {
 public:
  /// Calculates summary statistics for the features.
  static std::shared_ptr<const metrics::Scores> calculate_feature_stats(
      const Pipeline& _pipeline, const FittedPipeline& _fitted,
      const containers::NumericalFeatures _features,
      const Poco::JSON::Object& _cmd,
      const containers::DataFrame& _population_df);

  /// Calculates the column importances.
  static std::pair<std::vector<helpers::ColumnDescription>,
                   std::vector<std::vector<Float>>>
  column_importances(const Pipeline& _pipeline, const FittedPipeline& _fitted);

  /// Returns the column importances as JSON objects.
  static Poco::JSON::Object column_importances_as_obj(
      const Pipeline& _pipeline, const FittedPipeline& _fitted);

  /// Calculate the feature importances.
  static std::vector<std::vector<Float>> feature_importances(
      const FittedPipeline& _fitted,
      const std::vector<std::vector<fct::Ref<const predictors::Predictor>>>&
          _predictors);

  /// Extracts the feature importances as a Poco::JSON::Object.
  static Poco::JSON::Object feature_importances_as_obj(
      const FittedPipeline& _fitted);

  /// Extracts the feature names as a Poco::JSON::Object.
  static Poco::JSON::Object feature_names_as_obj(const FittedPipeline& _fitted);

  /// Scores the pipeline.
  static std::pair<fct::Ref<const metrics::Scores>, Poco::JSON::Object> score(
      const Pipeline& _pipeline, const FittedPipeline& _fitted,
      const containers::DataFrame& _population_df,
      const std::string& _population_name,
      const containers::NumericalFeatures& _yhat);

 private:
  /// Calculates the column importances for the autofeatures.
  static void column_importances_auto(
      const FittedPipeline& _fitted,
      const std::vector<std::vector<Float>>& _f_importances,
      std::vector<helpers::ImportanceMaker>* _importance_makers);

  /// Calculates the columns importances for the manual features.
  static void column_importances_manual(
      const Pipeline& _pipeline, const FittedPipeline& _fitted,
      const std::vector<std::vector<Float>>& _f_importances,
      std::vector<helpers::ImportanceMaker>* _importance_makers);

  /// Extracts the columns descriptions
  static void extract_coldesc(
      const std::map<helpers::ColumnDescription, Float>& _column_importances,
      std::vector<helpers::ColumnDescription>* _coldesc);

  /// Extracts the importance values.
  static void extract_importance_values(
      const std::map<helpers::ColumnDescription, Float>& _column_importances,
      std::vector<std::vector<Float>>* _all_column_importances);

  /// Fills the columns descriptions that have not been assigned with importance
  /// value 0.0.
  static void fill_zeros(std::vector<helpers::ImportanceMaker>* _f_importances);

  /// The importances factors are needed for backpropagating the feature
  /// importances to the column importances.
  static std::vector<Float> make_importance_factors(
      const size_t _num_features, const std::vector<size_t>& _autofeatures,
      const std::vector<Float>::const_iterator _begin,
      const std::vector<Float>::const_iterator _end);

  /// Expresses a nested vector as a transposed Poco::JSON::Array::Ptr.
  static Poco::JSON::Array::Ptr transpose(
      const std::vector<std::vector<Float>>& _original);
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_SCORE_HPP_
