// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_TABLEHOLDER_HPP_
#define HELPERS_TABLEHOLDER_HPP_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "helpers/DataFrame.hpp"
#include "helpers/DataFrameView.hpp"
#include "helpers/FeatureContainer.hpp"
#include "helpers/Placeholder.hpp"
#include "helpers/RowIndexContainer.hpp"
#include "helpers/TableHolderParams.hpp"
#include "helpers/WordIndexContainer.hpp"

namespace helpers {

class TableHolder {
 public:
  typedef typename DataFrame::AdditionalColumns AdditionalColumns;

  typedef typename RowIndexContainer::RowIndices RowIndices;

  typedef typename WordIndexContainer::WordIndices WordIndices;

  explicit TableHolder(const TableHolderParams& _params);

  ~TableHolder();

 private:
  /// Adds the text fields to the peripheral tables.
  static std::vector<DataFrame> add_text_fields_to_peripheral_tables(
      const std::vector<DataFrame>& _original,
      const TableHolderParams& _params);

  /// Counts the number of peripheral tables that have been created from text
  /// fields.
  static size_t count_text(const std::vector<DataFrame>& _peripheral);

  /// Identifies the index for the associated peripheral table.
  static size_t find_peripheral_ix(
      const std::vector<std::string>& _peripheral_names,
      const std::string& _name);

  /// Generates additional numerical columns to add to a data frame.
  static std::vector<Column<Float>> make_additional_columns(
      const std::optional<const FeatureContainer>& _feature_container,
      const size_t _i);

  /// Generates a new output table to be used by a subtable.
  static DataFrameView make_output(const TableHolderParams& _params,
                                   const size_t _i, const size_t _j);

  /// Creates the row indices for the subtables.
  static std::shared_ptr<const std::vector<size_t>> make_subrows(
      const DataFrameView& _population_subview,
      const DataFrame& _peripheral_subview);

  /// Creates the main tables during construction.
  static std::vector<DataFrameView> parse_main_tables(
      const TableHolderParams& _params);

  /// Creates the peripheral tables during construction.
  static std::vector<DataFrame> parse_peripheral_tables(
      const TableHolderParams& _params);

  /// Parses the propositionalization flag in the Placeholder
  static std::vector<bool> parse_propositionalization(
      const Placeholder& _placeholder, const size_t _expected_size);

  /// Creates the subtables during construction.
  static std::vector<std::optional<TableHolder>> parse_subtables(
      const TableHolderParams& _params);

  // ------------------------------

 public:
  /// Trivial (const) accessor.
  const std::vector<DataFrameView>& main_tables() const { return main_tables_; }

  /// Trivial (const) accessor.
  const std::vector<DataFrame>& peripheral_tables() const {
    return peripheral_tables_;
  }

  /// Trivial (const) accessor.
  const std::vector<bool>& propositionalization() const {
    return propositionalization_;
  }

  /// Trivial (const) accessor.
  const std::vector<std::optional<TableHolder>>& subtables() const {
    return subtables_;
  }

  /// Extracts the wors indices from the tables.
  WordIndexContainer word_indices() const;

  // ------------------------------

 private:
  /// The TableHolder has a population table, which may or may not be
  /// identical with the actual population table.
  const std::vector<DataFrameView> main_tables_;

  /// The TableHolder can have peripheral tables.
  const std::vector<DataFrame> peripheral_tables_;

  /// Whether we want to use propsitionalization on a particular relationship.
  const std::vector<bool> propositionalization_;

  /// The TableHolder may or may not have subtables.
  const std::vector<std::optional<TableHolder>> subtables_;
};

// ----------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_TABLEHOLDER_HPP_
