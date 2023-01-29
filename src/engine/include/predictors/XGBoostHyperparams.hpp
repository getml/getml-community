// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_XGBOOSTHYPERPARMS_HPP_
#define PREDICTORS_XGBOOSTHYPERPARMS_HPP_

#include <Poco/JSON/Object.h>
#include <xgboost/c_api.h>

#include <cstddef>

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "predictors/Float.hpp"
#include "predictors/Int.hpp"

namespace predictors {

/// Hyperparameters for XGBoost.
struct XGBoostHyperparams {
  /// L1 regularization term on weights
  using f_alpha = fct::Field<"alpha_", Float>;

  /// Specify which booster to use: gbtree, gblinear or dart.
  using f_booster =
      fct::Field<"booster_", fct::Literal<"gbtree", "gblinear", "dart">>;

  /// Subsample ratio of columns for each split, in each level.
  using f_colsample_bylevel = fct::Field<"colsample_bylevel_", Float>;

  /// Subsample ratio of columns when constructing each tree.
  using f_colsample_bytree = fct::Field<"colsample_bytree_", Float>;

  /// Maximum number of no improvements to trigger early stopping.
  using f_early_stopping_rounds = fct::Field<"early_stopping_rounds_", size_t>;

  /// Boosting learning rate
  using f_eta = fct::Field<"eta_", Float>;

  /// Whether you want to use external_memory_ (only as an affect when
  /// memory mapping is used).
  using f_external_memory = fct::Field<"external_memory_", bool>;

  /// Minimum loss reduction required to make a further partition on a leaf
  /// node of the tree.
  using f_gamma = fct::Field<"gamma_", Float>;

  /// L2 regularization term on weights
  using f_lambda = fct::Field<"lambda_", Float>;

  /// Maximum delta step we allow each tree’s weight estimation to be.
  using f_max_delta_step = fct::Field<"max_delta_step_", Float>;

  /// Maximum tree depth for base learners
  using f_max_depth = fct::Field<"max_depth_", size_t>;

  /// Minimum sum of instance weight needed in a child
  using f_min_child_weights = fct::Field<"min_child_weights_", Float>;

  /// Number of iterations (number of trees in boosted ensemble)
  using f_n_iter = fct::Field<"n_iter_", size_t>;

  /// For dart only. Which normalization to use.
  using f_normalize_type =
      fct::Field<"normalize_type_", fct::Literal<"tree", "forest">>;

  /// ...
  using f_num_parallel_tree = fct::Field<"num_parallel_tree_", size_t>;

  /// Number of parallel threads used to run xgboost
  using f_nthread = fct::Field<"nthread_", Int>;

  /// The objective for the learning function.
  using f_objective =
      fct::Field<"objective_", fct::Literal<"reg:linear", "reg:squarederror",
                                            "reg:logistic", "binary:logistic",
                                            "binary:logitraw", "reg:tweedie">>;

  /// For dart only. If true, at least one tree will be dropped out.
  using f_one_drop = fct::Field<"one_drop_", bool>;

  /// For dart only. Dropout rate.
  using f_rate_drop = fct::Field<"rate_drop_", Float>;

  /// For dart only. Whether you want to use "uniform" or "weighted"
  /// sampling
  using f_sample_type =
      fct::Field<"sample_type_", fct::Literal<"uniform", "weighted">>;

  /// Whether to print messages while running boosting
  using f_silent = fct::Field<"silent_", bool>;

  /// For dart only. Probability of skipping dropout.
  using f_skip_drop = fct::Field<"skip_drop_", Float>;

  /// Subsample ratio of the training instance.
  using f_subsample = fct::Field<"subsample_", Float>;

  using RecursiveType = fct::NamedTuple<
      f_alpha, f_booster, f_colsample_bylevel, f_colsample_bytree,
      f_early_stopping_rounds, f_eta, f_external_memory, f_gamma, f_lambda,
      f_max_delta_step, f_max_depth, f_min_child_weights, f_n_iter,
      f_normalize_type, f_num_parallel_tree, f_nthread, f_objective, f_one_drop,
      f_rate_drop, f_sample_type, f_silent, f_skip_drop, f_subsample>;

  // TODO: Replace this quick fix.
  XGBoostHyperparams(const Poco::JSON::Object &_json_obj)
      : val_(json::from_json<RecursiveType>(_json_obj)) {}

  ~XGBoostHyperparams() = default;

  /// Applies the hyperparameters to an XGBoost Handler
  template <int _i = 0>
  inline void apply(BoosterHandle _handle) const {
    using Fields = typename RecursiveType::Fields;

    if constexpr (_i == std::tuple_size_v<Fields>) {
      return;
    } else {
      using FieldType = typename std::tuple_element<_i, Fields>::type;

      const auto name_with_underscore = FieldType::name_.str();

      const auto name =
          name_with_underscore.substr(0, name_with_underscore.size() - 1);

      if (name == "early_stopping_rounds" || name == "external_memory" ||
          name == "n_iter") {
        apply<_i + 1>(_handle);
        return;
      }

      using Type = typename FieldType::Type;

      constexpr bool is_numeric = std::is_same<Type, Float>() ||
                                  std::is_same<Type, Int>() ||
                                  std::is_same<Type, size_t>();

      constexpr bool is_bool = std::is_same<Type, bool>();

      const auto value = val_.get<_i>();

      if constexpr (is_numeric) {
        XGBoosterSetParam(_handle, name.c_str(), std::to_string(value).c_str());
      } else if constexpr (is_bool) {
        XGBoosterSetParam(_handle, name.c_str(), value ? "1" : "0");
      } else {
        XGBoosterSetParam(_handle, name.c_str(), value.name().c_str());
      }

      apply<_i + 1>(_handle);
    }
  }

  /// The underlying named tuple.
  RecursiveType val_;
};

}  // namespace predictors

#endif  // PREDICTORS_XGBOOSTHYPERPARMS_HPP_
