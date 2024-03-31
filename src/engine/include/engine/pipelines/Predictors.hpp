// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_PREDICTORS_HPP_
#define ENGINE_PIPELINES_PREDICTORS_HPP_

#include <vector>

#include "fct/fct.hpp"
#include "predictors/predictors.hpp"

namespace engine {
namespace pipelines {

struct Predictors {
  /// Checked access.
  auto at(const size_t _i) const { return predictors_.at(_i); }

  /// Unchecked access.
  auto operator[](const size_t _i) const { return predictors_[_i]; }

  /// The number of predictors
  size_t size() const { return predictors_.size(); }

  /// Returns the names of the autofeatures.
  std::vector<std::string> autofeature_names() const;

  /// Returns the names of the autofeatures, the numerical manual features and
  /// the categorical manual features.
  std::tuple<std::vector<std::string>, std::vector<std::string>,
             std::vector<std::string>>
  feature_names() const;

  /// Calculates the number of automated and manual features used.
  size_t num_features() const;

  /// Calculates the number of predictors per set.
  size_t num_predictors_per_set() const;

  /// Pimpl for the predictors.
  rfl::Ref<const predictors::PredictorImpl> impl_;

  /// The actual predictors used.
  std::vector<std::vector<rfl::Ref<const predictors::Predictor>>> predictors_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_PREDICTORS_HPP_

