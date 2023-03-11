// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_FEATURELEARNERS_ABSTRACTFEATURELEARNER_HPP_
#define ENGINE_FEATURELEARNERS_ABSTRACTFEATURELEARNER_HPP_

#include <map>
#include <memory>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/featurelearners/FitParams.hpp"
#include "engine/featurelearners/TransformParams.hpp"

namespace engine {
namespace featurelearners {

class AbstractFeatureLearner {
 public:
  static constexpr Int USE_ALL_TARGETS = -1;
  static constexpr Int IGNORE_TARGETS = -2;

  static constexpr const char* FASTPROP = "FastProp";
  static constexpr const char* MULTIREL = "Multirel";
  static constexpr const char* RELBOOST = "Relboost";
  static constexpr const char* RELMT = "RelMT";

 public:
  AbstractFeatureLearner() {}

  virtual ~AbstractFeatureLearner() = default;

 public:
  /// Calculates the column importances for this ensemble.
  virtual std::map<helpers::ColumnDescription, Float> column_importances(
      const std::vector<Float>& _importance_factors) const = 0;

  /// Creates a deep copy.
  virtual std::shared_ptr<AbstractFeatureLearner> clone() const = 0;

  /// Returns the fingerprint of the feature learner (necessary to build
  /// the dependency graphs).
  virtual commands::Fingerprint fingerprint() const = 0;

  /// Fits the model.
  virtual void fit(const FitParams& _params) = 0;

  /// Whether this is a classification problem.
  virtual bool is_classification() const = 0;

  /// Loads the feature learner from a file designated by _fname.
  virtual void load(const std::string& _fname) = 0;

  /// Returns the placeholder not as passed by the user, but as seen by the
  /// feature learner (the difference matters for time series).
  virtual helpers::Placeholder make_placeholder() const = 0;

  /// Returns the number of features in the feature learner.
  virtual size_t num_features() const = 0;

  /// Determines whether the population table needs targets during
  /// transform (only for time series that include autoregression).
  virtual bool population_needs_targets() const = 0;

  /// Whether the feature learner is for the premium version only.
  virtual bool premium_only() const = 0;

  /// Saves the Model in JSON format, if applicable
  virtual void save(const std::string& _fname) const = 0;

  /// Whether the feature learner is to be silent.
  virtual bool silent() const = 0;

  /// Whether the feature learner supports multiple targets.
  virtual bool supports_multiple_targets() const = 0;

  /// Return features as SQL code.
  virtual std::vector<std::string> to_sql(
      const helpers::StringIterator& _categories, const bool _targets,
      const bool _subfeatures,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _prefix) const = 0;

  /// Generate features.
  virtual containers::NumericalFeatures transform(
      const TransformParams& _params) const = 0;

  /// Returns a string describing the type of the feature learner.
  virtual std::string type() const = 0;
};

}  // namespace featurelearners
}  // namespace engine

#endif  // ENGINE_FEATURELEARNERS_ABSTRACTFEATURELEARNER_HPP_

