// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/WordIndexContainer.hpp"

namespace helpers {

WordIndexContainer::WordIndexContainer(
    const DataFrame& _population, const std::vector<DataFrame>& _peripheral,
    const VocabularyContainer& _vocabulary_container) {
  assert_msg(_vocabulary_container.peripheral().size() == _peripheral.size(),
             "_vocabulary_container.peripheral().size(): " +
                 std::to_string(_vocabulary_container.peripheral().size()) +
                 ", _peripheral.size(): " + std::to_string(_peripheral.size()));

  for (size_t i = 0; i < _peripheral.size(); ++i) {
    peripheral_.push_back(make_word_indices(
        _vocabulary_container.peripheral().at(i), _peripheral.at(i)));
  }

  population_ =
      make_word_indices(_vocabulary_container.population(), _population);
}

// ----------------------------------------------------------------------------

WordIndexContainer::WordIndexContainer(
    const WordIndices& _population, const std::vector<WordIndices>& _peripheral)
    : peripheral_(_peripheral), population_(_population) {}

// ----------------------------------------------------------------------------

WordIndexContainer::~WordIndexContainer() = default;

// ----------------------------------------------------------------------------

VocabularyContainer WordIndexContainer::vocabulary() const {
  const auto get_vocab =
      [](const std::shared_ptr<const textmining::WordIndex>& _word_index) {
        assert_true(_word_index);
        return _word_index->vocabulary_ptr();
      };

  const auto extract_vocab_for_df =
      [get_vocab](const WordIndices& _word_indices) {
        return _word_indices | std::views::transform(get_vocab) |
               std::ranges::to<std::vector>();
      };

  const auto population = extract_vocab_for_df(population_);

  const auto peripheral = peripheral_ |
                          std::views::transform(extract_vocab_for_df) |
                          std::ranges::to<std::vector>();

  return VocabularyContainer(population, peripheral);
}

// ----------------------------------------------------------------------------

typename WordIndexContainer::WordIndices WordIndexContainer::make_word_indices(
    const VocabForDf& _vocabulary, const DataFrame& _df) const {
  assert_true(_df.text_.size() == _vocabulary.size());

  const auto make_index = [&_df, &_vocabulary](const size_t ix) {
    const auto& col = _df.text_.at(ix);
    const auto& voc = _vocabulary.at(ix);
    assert_msg(voc, "ix: " + std::to_string(ix));
    return std::make_shared<const textmining::WordIndex>(
        fct::Range(col.begin(), col.end()), voc);
  };

  return std::views::iota(0uz, _df.text_.size()) |
         std::views::transform(make_index) | std::ranges::to<std::vector>();
}

// ----------------------------------------------------------------------------
}  // namespace helpers
