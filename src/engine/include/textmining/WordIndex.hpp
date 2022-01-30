#ifndef TEXTMINING_WORDINDEX_HPP_
#define TEXTMINING_WORDINDEX_HPP_

// -------------------------------------------------------------------------

#include <memory>
#include <utility>
#include <vector>

// -------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "stl/stl.hpp"
#include "strings/strings.hpp"

// -------------------------------------------------------------------------

#include "textmining/Int.hpp"
#include "textmining/Vocabulary.hpp"

// -------------------------------------------------------------------------

namespace textmining {

class WordIndex {
 public:
  template <class RangeType>
  WordIndex(
      const RangeType& _range,
      const std::shared_ptr<const std::vector<strings::String>>& _vocabulary)
      : vocabulary_(_vocabulary) {
    std::tie(indptr_, words_) = make_indptr_and_words(_range);
  }

  ~WordIndex() = default;

 public:
  /// Returns the word range for the _i'th row
  stl::Range<const Int*> range(const size_t _i) const {
    assert_msg(_i + 1 < indptr_.size(),
               "_i: " + std::to_string(_i) +
                   ", indptr_.size(): " + std::to_string(indptr_.size()));
    assert_true(indptr_[_i + 1] <= words_.size());
    return stl::Range(words_.data() + indptr_[_i],
                      words_.data() + indptr_[_i + 1]);
  }

  /// The number of rows.
  size_t nrows() const {
    assert_true(indptr_.size() != 0);
    return indptr_.size() - 1;
  }

  /// The size of the vocabulary.
  size_t size() const { return vocabulary().size(); }

  /// Trivial (const) accessor.
  const std::vector<strings::String>& vocabulary() const {
    assert_true(vocabulary_);
    return *vocabulary_;
  }

  /// Trivial (const) accessor.
  std::shared_ptr<const std::vector<strings::String>> vocabulary_ptr() const {
    assert_true(vocabulary_);
    return vocabulary_;
  }

  /// Trivial (const) accessor.
  const std::vector<Int>& words() const { return words_; }

 private:
  /// Generates the index and the indptr during construction.
  template <class RangeType>
  std::pair<std::vector<size_t>, std::vector<Int>> make_indptr_and_words(
      const RangeType& _range) const {
    const auto voc_range = stl::Range<const strings::String*>(
        vocabulary().data(), vocabulary().data() + vocabulary().size());

    const auto voc_map = Vocabulary::to_map(voc_range);

    const auto to_number = [&voc_map](const strings::String& word) -> Int {
      const auto it = voc_map.find(word);
      if (it == voc_map.end()) {
        return -1;
      }
      return it->second;
    };

    const auto in_vocabulary = [](const Int val) -> bool { return val >= 0; };

    auto indptr = std::vector<size_t>({0});

    auto words = std::vector<Int>(0);

    for (const auto& textfield : _range) {
      const auto processed = Vocabulary::process_text_field(textfield);

      auto range = processed | VIEWS::transform(to_number) |
                   VIEWS::filter(in_vocabulary);

      size_t num_words = 0;

      for (const auto& word_ix : range) {
        words.push_back(word_ix);
        ++num_words;
      }

      std::sort(words.begin() + indptr.back(),
                words.begin() + indptr.back() + num_words);

      indptr.push_back(indptr.back() + num_words);
    }

    return std::make_pair(indptr, words);
  }

 private:
  /// Indicates the beginning and of each word in rownums.
  std::vector<size_t> indptr_;

  /// The vocabulary.
  std::shared_ptr<const std::vector<strings::String>> vocabulary_;

  /// Indicates the words contained in the text field.
  std::vector<Int> words_;
};

// -------------------------------------------------------------------------
}  // namespace textmining

#endif  // ORDINDEX_HPP_
