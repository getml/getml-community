// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_CREATESUBVIEWPARAMS_HPP_
#define HELPERS_CREATESUBVIEWPARAMS_HPP_

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "helpers/ColumnView.hpp"
#include "helpers/DataFrameParams.hpp"

namespace helpers {

struct CreateSubviewParams {
 public:
  typedef DataFrameParams::AdditionalColumns AdditionalColumns;

  typedef DataFrameParams::RowIndices RowIndices;

  typedef DataFrameParams::WordIndices WordIndices;

  /// Additional columns to be added to the DataFrame.
  const AdditionalColumns additional_ = {};

  /// Whether we want to allow lagged targets.
  const bool allow_lagged_targets_ = false;

  /// The name of the join key to be used.
  const std::string join_key_ = "";

  /// The function needed to make the colname (only used when columns are not
  /// found, for better error messages)
  const std::optional<std::function<std::string(std::string)>>
      make_staging_table_colname_ = std::nullopt;

  /// A column view on the join keys in the population table, if this is a
  /// peripheral table. This is needed to build the time series index.
  const std::optional<ColumnView<Int, std::vector<size_t>>>
      population_join_keys_ = std::nullopt;

  /// Index returning rows for each word.
  const RowIndices row_indices_ = {};

  /// The name of the lower time stamp.
  const std::string time_stamp_ = "";

  /// The name of the upper time stamp.
  const std::string upper_time_stamp_ = "";

  /// Index returning words for each row.
  const WordIndices word_indices_ = {};
};
}  // namespace helpers

// -------------------------------------------------------------------------

#endif  // HELPERS_CREATESUBVIEWPARAMS_HPP_

