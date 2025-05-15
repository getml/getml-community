// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_ROWINDEXCONTAINER_HPP_
#define HELPERS_ROWINDEXCONTAINER_HPP_

#include "helpers/VocabularyContainer.hpp"
#include "helpers/WordIndexContainer.hpp"

#include <memory>
#include <vector>

namespace helpers {

class RowIndexContainer {
 public:
  typedef typename VocabularyContainer::VocabForDf VocabForDf;

  typedef std::vector<std::shared_ptr<const textmining::RowIndex>> RowIndices;

  typedef typename WordIndexContainer::WordIndices WordIndices;

 public:
  explicit RowIndexContainer(const WordIndexContainer& _word_index_container);

  RowIndexContainer(const RowIndices& _population,
                    const std::vector<RowIndices>& _peripheral);

  ~RowIndexContainer() = default;

 public:
  /// Trivial (const) accessor
  const std::vector<RowIndices>& peripheral() const { return peripheral_; }

  /// Trivial (const) accessor
  const RowIndices& population() const { return population_; }

 private:
  /// Generates the row indices for all text columns in a data frame.
  RowIndices make_row_indices(const WordIndices& _word_indices) const;

 private:
  /// The vocabulary for the peripheral tables.
  std::vector<RowIndices> peripheral_;

  /// The vocabulary for the population table.
  RowIndices population_;
};

}  // namespace helpers

#endif  // HELPERS_ROWINDEXCONTAINER_HPP_
