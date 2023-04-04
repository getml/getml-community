// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_HYPERPARAMETERS_HPP_
#define FASTPROP_HYPERPARAMETERS_HPP_

#include "fastprop/Float.hpp"
#include "fastprop/Int.hpp"
#include "fastprop/enums/Aggregation.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "json/json.hpp"

namespace fastprop {

/// Describes the aggregations that may be used
using f_aggregations =
    fct::Field<"aggregation_", std::vector<enums::Aggregation>>;

/// Size of the moving time windows.
using f_delta_t = fct::Field<"delta_t_", Float>;

/// The loss function (FastProp is completely unsupervised, so we simply
/// have this for consistency).
using f_loss_function = fct::Field<"loss_function_", std::string>;

/// The maximum lag.
using f_max_lag = fct::Field<"max_lag_", size_t>;

/// The minimum document frequency required for a string to become part of
/// the vocabulary.
using f_min_df = fct::Field<"min_df_", size_t>;

/// The number of categories from which we would like to extract numerical
/// features.
using f_n_most_frequent = fct::Field<"n_most_frequent_", size_t>;

/// The maximum number of features generated.
using f_num_features = fct::Field<"num_features_", size_t>;

/// The number of threads we want to use
using f_num_threads = fct::Field<"num_threads_", Int>;

/// The sampling factor to use. Set to 1 for no sampling.
using f_sampling_factor = fct::Field<"sampling_factor_", Float>;

/// Whether we want logging.
using f_silent = fct::Field<"silent_", bool>;

/// Defines the type
using f_type = fct::Field<"type_", fct::Literal<"FastProp">>;

/// The maximum size of the vocabulary.
using f_vocab_size = fct::Field<"vocab_size_", size_t>;

struct Hyperparameters {
  static constexpr const char* CROSS_ENTROPY_LOSS = "CrossEntropyLoss";
  static constexpr const char* SQUARE_LOSS = "SquareLoss";

  using NamedTupleType =
      fct::NamedTuple<f_aggregations, f_delta_t, f_loss_function, f_max_lag,
                      f_min_df, f_n_most_frequent, f_num_features,
                      f_num_threads, f_sampling_factor, f_silent, f_type,
                      f_vocab_size>;

  Hyperparameters(const NamedTupleType& _val) : val_(_val) {}

  ~Hyperparameters() = default;

  /// Trivial accessor
  auto loss_function() const { return val_.get<"loss_function_">(); }

  /// Trivial accessor
  auto min_df() const { return val_.get<"min_df_">(); }

  /// Trivial accessor.
  auto named_tuple() const { return val_; }

  /// Trivial accessor
  bool silent() const { return val_.get<"silent_">(); }

  /// Trivial accessor
  auto vocab_size() const { return val_.get<"vocab_size_">(); }

  /// Usually used to break a recursive definition, but in
  /// this case it is used for backwards compabatability.
  const NamedTupleType val_;
};

}  // namespace fastprop

#endif  // FASTPROP_HYPERPARAMETERS_HPP_
