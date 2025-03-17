// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_WORDINDEXCONTAINER_HPP_
#define HELPERS_WORDINDEXCONTAINER_HPP_

#include "helpers/VocabularyContainer.hpp"

#include <memory>
#include <vector>

namespace helpers {

class WordIndexContainer {
 public:
  typedef typename VocabularyContainer::VocabForDf VocabForDf;

  typedef std::vector<std::shared_ptr<const textmining::WordIndex>> WordIndices;

  WordIndexContainer(const DataFrame& _population,
                     const std::vector<DataFrame>& _peripheral,
                     const VocabularyContainer& _vocabulary_container);

  WordIndexContainer(const WordIndices& _population,
                     const std::vector<WordIndices>& _peripheral);

  ~WordIndexContainer() = default;

 public:
  /// Trivial (const) accessor
  const std::vector<WordIndices>& peripheral() const { return peripheral_; }

  /// Trivial (const) accessor
  const WordIndices& population() const { return population_; }

 public:
  /// Extracts the vocabulary container from the individual rows.
  VocabularyContainer vocabulary() const;

 private:
  /// Generates the word indices for all text columns in a data frame.
  WordIndices make_word_indices(const VocabForDf& _vocabulary,
                                const DataFrame& _df) const;

 private:
  /// The vocabulary for the peripheral tables.
  std::vector<WordIndices> peripheral_;

  /// The vocabulary for the population table.
  WordIndices population_;
};

}  // namespace helpers

#endif  // HELPERS_WORDINDEXCONTAINER_HPP_
