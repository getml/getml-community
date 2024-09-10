// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef HELPERS_TABLEHOLDERPARAMS_HPP_
#define HELPERS_TABLEHOLDERPARAMS_HPP_

#include <functional>
#include <string>
#include <vector>

#include "helpers/DataFrameView.hpp"
#include "helpers/FeatureContainer.hpp"
#include "helpers/Placeholder.hpp"
#include "helpers/RowIndexContainer.hpp"
#include "helpers/WordIndexContainer.hpp"

namespace helpers {

struct TableHolderParams {
  /// Contains subfeatures (only relevant for the snowflake data schema).
  const std::optional<const FeatureContainer> feature_container_ = std::nullopt;

  /// Generates a colname in human-readable form (relevant for error messages
  /// when a colname has not been found).
  const std::function<std::string(std::string)> make_staging_table_colname_;

  /// The data frame serving as the peripheral tables for the purpose of this
  /// TableHolder.
  const std::vector<DataFrame> peripheral_;

  /// The names of the peripheral tables, as referred to in the placeholder.
  const std::vector<std::string> peripheral_names_;

  /// The main placeholder that contains the relational tree.
  const Placeholder placeholder_;

  /// The data frame serving as the population table for the purpose of this
  /// TableHolder.
  const DataFrameView population_;

  /// Maps words to rows.
  const std::optional<RowIndexContainer> row_index_container_ = std::nullopt;

  /// Maps rows to words.
  const std::optional<WordIndexContainer> word_index_container_ = std::nullopt;
};
}  // namespace helpers

#endif  // HELPERS_TABLEHOLDERPARAMS_HPP_

