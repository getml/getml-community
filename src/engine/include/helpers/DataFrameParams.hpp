// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_DATAFRAMEPARAMS_HPP_
#define HELPERS_DATAFRAMEPARAMS_HPP_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "helpers/Column.hpp"
#include "helpers/Float.hpp"
#include "helpers/Index.hpp"
#include "helpers/Int.hpp"
#include "textmining/textmining.hpp"
#include "tsindex/tsindex.hpp"

namespace helpers {

// -------------------------------------------------------------------------

struct DataFrameParams {
  // ---------------------------------------------------------------------

  typedef Column<Float> FloatColumnType;

  typedef Column<Int> IntColumnType;

  typedef std::vector<Column<Float>> AdditionalColumns;

  typedef std::vector<std::shared_ptr<const textmining::RowIndex>> RowIndices;

  typedef Column<strings::String> StringColumnType;

  typedef std::vector<std::shared_ptr<const textmining::WordIndex>> WordIndices;

  // ---------------------------------------------------------------------

  /// Returns a deep copy of the params, but with a new set of time stamps.
  DataFrameParams with_time_stamps(
      const std::vector<Column<Float>>& _time_stamps) const {
    return DataFrameParams{.categoricals_ = categoricals_,
                           .discretes_ = discretes_,
                           .indices_ = indices_,
                           .join_keys_ = join_keys_,
                           .name_ = name_,
                           .numericals_ = numericals_,
                           .row_indices_ = row_indices_,
                           .targets_ = targets_,
                           .text_ = text_,
                           .time_stamps_ = _time_stamps,
                           .ts_index_ = ts_index_,
                           .word_indices_ = word_indices_};
  }

  /// Returns a deep copy of the params, but with a new ts_index_.
  DataFrameParams with_ts_index(
      const std::shared_ptr<tsindex::Index>& _ts_index) const {
    return DataFrameParams{.categoricals_ = categoricals_,
                           .discretes_ = discretes_,
                           .indices_ = indices_,
                           .join_keys_ = join_keys_,
                           .name_ = name_,
                           .numericals_ = numericals_,
                           .row_indices_ = row_indices_,
                           .targets_ = targets_,
                           .text_ = text_,
                           .time_stamps_ = time_stamps_,
                           .ts_index_ = _ts_index,
                           .word_indices_ = word_indices_};
  }

  // ---------------------------------------------------------------------

  /// Pointer to categorical columns.
  const std::vector<Column<Int>> categoricals_;

  /// Pointer to discrete columns.
  const std::vector<Column<Float>> discretes_;

  /// Indices assiciated with join keys.
  const std::vector<std::shared_ptr<Index>> indices_;

  /// Join keys of this data frame.
  const std::vector<Column<Int>> join_keys_;

  /// Name of the data frame.
  const std::string name_;

  /// Pointer to numerical columns.
  const std::vector<Column<Float>> numericals_;

  /// Index returning rows for each word.
  const RowIndices row_indices_;

  /// Pointer to target column.
  const std::vector<Column<Float>> targets_;

  /// Pointer to text column.
  const std::vector<Column<strings::String>> text_;

  /// Time stamps of this data frame.
  const std::vector<Column<Float>> time_stamps_;

  /// An index for the time stamps, which can increase performance in certain
  /// scenarios.
  const std::shared_ptr<tsindex::Index> ts_index_;

  /// Index returning words for each row.
  const WordIndices word_indices_;
};

// ----------------------------------------------------------------------------

}  // namespace helpers

// ----------------------------------------------------------------------------

#endif  // HELPERS_DATAFRAMEPARAMS_HPP_
