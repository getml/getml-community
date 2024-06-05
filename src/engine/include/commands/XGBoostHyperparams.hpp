// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_XGBOOSTHYPERPARMS_HPP_
#define COMMANDS_XGBOOSTHYPERPARMS_HPP_

#include <xgboost/c_api.h>

#include <cstddef>

#include "commands/Float.hpp"
#include "commands/Int.hpp"
#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/named_tuple_t.hpp>

namespace commands {

/// Hyperparameters for XGBoost.
struct XGBoostHyperparams {
  /// Applies the hyperparameters to an XGBoost Handler
  template <int _i = 0>
  inline void apply(BoosterHandle _handle) const {
    using ReflectionType = rfl::named_tuple_t<XGBoostHyperparams>;
    using Fields = typename ReflectionType::Fields;

    if constexpr (_i == std::tuple_size_v<Fields>) {
      return;
    } else {
      using FieldType = typename std::tuple_element<_i, Fields>::type;

      const auto name_with_underscore = FieldType::name_.str();

      const auto name =
          name_with_underscore.substr(0, name_with_underscore.size() - 1);

      if (name == "type_" || name == "early_stopping_rounds" ||
          name == "external_memory" || name == "n_iter") {
        apply<_i + 1>(_handle);
        return;
      }

      using Type = typename FieldType::Type;

      constexpr bool is_numeric = std::is_same<Type, Float>() ||
                                  std::is_same<Type, Int>() ||
                                  std::is_same<Type, size_t>();

      constexpr bool is_bool = std::is_same<Type, bool>();

      const auto value = rfl::get<_i>(rfl::to_named_tuple(*this));

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

  /// L1 regularization term on weights
  rfl::Field<"reg_alpha_", Float> alpha;

  /// Specify which booster to use: gbtree, gblinear or dart.
  rfl::Field<"booster_", rfl::Literal<"gbtree", "gblinear", "dart">> booster;

  /// Subsample ratio of columns for each split, in each level.
  rfl::Field<"colsample_bylevel_", Float> colsample_bylevel;

  /// Subsample ratio of columns when constructing each tree.
  rfl::Field<"colsample_bytree_", Float> f_colsample_bytree;

  /// Maximum number of no improvements to trigger early stopping.
  rfl::Field<"early_stopping_rounds_", size_t> early_stopping_rounds;

  /// Boosting learning rate
  rfl::Field<"learning_rate_", Float> eta;

  /// Whether you want to use external_memory_ (only as an affect when
  /// memory mapping is used).
  rfl::Field<"external_memory_", bool> external_memory;

  /// Minimum loss reduction required to make a further partition on a leaf
  /// node of the tree.
  rfl::Field<"gamma_", Float> gamma;

  /// L2 regularization term on weights
  rfl::Field<"reg_lambda_", Float> lambda;

  /// Maximum delta step we allow each tree’s weight estimation to be.
  rfl::Field<"max_delta_step_", Float> max_delta_step;

  /// Maximum tree depth for base learners
  rfl::Field<"max_depth_", size_t> max_depth;

  /// Minimum sum of instance weight needed in a child
  rfl::Field<"min_child_weights_", Float> min_child_weights;

  /// Number of iterations (number of trees in boosted ensemble)
  rfl::Field<"n_estimators_", size_t> n_estimators;

  /// For dart only. Which normalization to use.
  rfl::Field<"normalize_type_", rfl::Literal<"tree", "forest">> normalize_type;

  /// ...
  rfl::Field<"num_parallel_tree_", size_t> num_parallel_tree;

  /// Number of parallel threads used to run xgboost
  rfl::Field<"n_jobs_", Int> nthread;

  /// The objective for the learning function.
  rfl::Field<"objective_",
             rfl::Literal<"reg:linear", "reg:squarederror", "reg:logistic",
                          "binary:logistic", "binary:logitraw", "reg:tweedie">>
      objective;

  /// For dart only. If true, at least one tree will be dropped out.
  rfl::Field<"one_drop_", bool> one_drop;

  /// For dart only. Dropout rate.
  rfl::Field<"rate_drop_", Float> rate_drop;

  /// For dart only. Whether you want to use "uniform" or "weighted"
  /// sampling
  rfl::Field<"sample_type_", rfl::Literal<"uniform", "weighted">> sample_type;

  /// Whether to print messages while running boosting
  rfl::Field<"silent_", bool> silent;

  /// For dart only. Probability of skipping dropout.
  rfl::Field<"skip_drop_", Float> skip_drop;

  /// Subsample ratio of the training instance.
  rfl::Field<"subsample_", Float> subsample;

  /// Signifies this as XGBoost hyperparameters.
  rfl::Field<"type_", rfl::Literal<"XGBoostPredictor", "XGBoostClassifier",
                                   "XGBoostRegressor">>
      type;
};

}  // namespace commands

#endif  // COMMANDS_XGBOOSTHYPERPARMS_HPP_
