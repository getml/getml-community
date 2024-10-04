// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_HYPERPARAMETERS_HPP_
#define FASTPROP_HYPERPARAMETERS_HPP_

#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>

#include "fastprop/Float.hpp"
#include "fastprop/Int.hpp"
#include "fastprop/enums/Aggregation.hpp"

namespace fastprop {

struct Hyperparameters {
  static constexpr const char* CROSS_ENTROPY_LOSS = "CrossEntropyLoss";
  static constexpr const char* SQUARE_LOSS = "SquareLoss";

  /// Describes the aggregations that may be used
  rfl::Field<"aggregation_", std::vector<enums::Aggregation>> aggregation;

  /// Size of the moving time windows.
  rfl::Field<"delta_t_", Float> delta_t;

  /// The loss function (FastProp is completely unsupervised, so we simply
  /// have this for consistency).
  rfl::Field<"loss_function_", std::string> loss_function;

  /// The maximum lag.
  rfl::Field<"max_lag_", size_t> max_lag;

  /// The minimum document frequency required for a string to become part of
  /// the vocabulary.
  rfl::Field<"min_df_", size_t> min_df;

  /// The number of categories from which we would like to extract numerical
  /// features.
  rfl::Field<"n_most_frequent_", size_t> n_most_frequent;

  /// The maximum number of features generated.
  rfl::Field<"num_features_", size_t> num_features;

  /// The number of threads we want to use
  rfl::Field<"num_threads_", Int> num_threads;

  /// The sampling factor to use. Set to 1 for no sampling.
  rfl::Field<"sampling_factor_", Float> sampling_factor;

  /// Whether we want logging.
  rfl::Field<"silent_", bool> silent;

  /// Defines the type
  rfl::Field<"type_", rfl::Literal<"FastProp">> type;

  /// The maximum size of the vocabulary.
  rfl::Field<"vocab_size_", size_t> vocab_size;
};

}  // namespace fastprop

#endif  // FASTPROP_HYPERPARAMETERS_HPP_
