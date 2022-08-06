// Copyright 2022 The SQLNet Company GmbH
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
        auto range = _word_indices | VIEWS::transform(get_vocab);
        return fct::collect::vector<
            std::shared_ptr<const std::vector<strings::String>>>(range);
      };

  const auto population = extract_vocab_for_df(population_);

  auto range = peripheral_ | VIEWS::transform(extract_vocab_for_df);

  const auto peripheral = fct::collect::vector<VocabForDf>(range);

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

  const auto iota = fct::iota<size_t>(0, _df.text_.size());

  auto range = iota | VIEWS::transform(make_index);

  return fct::collect::vector<std::shared_ptr<const textmining::WordIndex>>(
      range);
}

// ----------------------------------------------------------------------------
}  // namespace helpers
