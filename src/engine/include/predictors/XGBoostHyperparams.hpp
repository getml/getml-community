// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef PREDICTORS_XGBOOSTHYPERPARMS_HPP_
#define PREDICTORS_XGBOOSTHYPERPARMS_HPP_

// ----------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------

#include <cstddef>
#include <string>

// ----------------------------------------------------------------------

#include "predictors/Float.hpp"
#include "predictors/Int.hpp"

// ----------------------------------------------------------------------

namespace predictors {

/// Hyperparameters for XGBoost.
struct XGBoostHyperparams {
  // -----------------------------------------

  XGBoostHyperparams(const Poco::JSON::Object &_json_obj);

  ~XGBoostHyperparams() = default;

  // -----------------------------------------

  /// L1 regularization term on weights
  const Float alpha_;

  /// Specify which booster to use: gbtree, gblinear or dart.
  const std::string booster_;

  /// Subsample ratio of columns for each split, in each level.
  const Float colsample_bylevel_;

  /// Subsample ratio of columns when constructing each tree.
  const Float colsample_bytree_;

  /// Maximum number of no improvements to trigger early stopping.
  const size_t early_stopping_rounds_;

  /// Boosting learning rate
  const Float eta_;

  /// Whether you want to use external_memory_ (only as an affect when
  /// memory mapping is used).
  const bool external_memory_;

  /// Minimum loss reduction required to make a further partition on a leaf
  /// node of the tree.
  const Float gamma_;

  /// L2 regularization term on weights
  const Float lambda_;

  /// Maximum delta step we allow each tree’s weight estimation to be.
  const Float max_delta_step_;

  /// Maximum tree depth for base learners
  const size_t max_depth_;

  /// Minimum sum of instance weight needed in a child
  const Float min_child_weights_;

  /// Number of iterations (number of trees in boosted ensemble)
  const size_t n_iter_;

  /// For dart only. Which normalization to use.
  const std::string normalize_type_;

  /// ...
  const size_t num_parallel_tree_;

  /// Number of parallel threads used to run xgboost
  const Int nthread_;

  /// The objective for the learning function.
  const std::string objective_;

  /// For dart only. If true, at least one tree will be dropped out.
  const bool one_drop_;

  /// For dart only. Dropout rate.
  const Float rate_drop_;

  /// For dart only. Whether you want to use "uniform" or "weighted"
  /// sampling
  const std::string sample_type_;

  /// Whether to print messages while running boosting
  const bool silent_;

  /// For dart only. Probability of skipping dropout.
  const Float skip_drop_;

  /// Subsample ratio of the training instance.
  const Float subsample_;
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_XGBOOSTHYPERPARMS_HPP_
