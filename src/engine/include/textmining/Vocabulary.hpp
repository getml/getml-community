// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef TEXTMINING_VOCABULARY_HPP_
#define TEXTMINING_VOCABULARY_HPP_

// -------------------------------------------------------------------------

#include <map>
#include <memory>
#include <ranges>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "fct/Range.hpp"
#include "fct/to.hpp"
#include "strings/String.hpp"
#include "textmining/Int.hpp"

// -------------------------------------------------------------------------

namespace textmining {
// -------------------------------------------------------------------------

class Vocabulary {
 public:
  /// Generates the vocabulary based on a column.
  template <class IteratorType>
  static std::shared_ptr<const std::vector<strings::String>> generate(
      const size_t _min_df, const size_t _max_size,
      const fct::Range<IteratorType> _range) {
    using Pair = std::pair<strings::String, size_t>;

    const auto df_count = count_df(_range);

    const auto count_greater_than_min_df = [_min_df](const Pair& p) -> bool {
      return p.second >= _min_df;
    };

    auto range = df_count | std::views::filter(count_greater_than_min_df) |
                 std::views::keys;

    auto vocab = [&range, &_max_size]() {
      if (_max_size > 0) {
        return range | std::views::take(_max_size) |
               fct::ranges::to<fct::shared_ptr::vector>();
      }
      return range | fct::ranges::to<fct::shared_ptr::vector>();
    }();

    std::ranges::sort(*vocab);

    return vocab;
  }

  /// Processes a single text field to extract a set of unique words.
  static std::set<std::string> process_text_field(
      const strings::String& _text_field);

  /// Splits a single text field to extract a vector of words.
  static std::vector<std::string> split_text_field(
      const strings::String& _text_field);

  /// Generates an unordered_map for the vocabulary.
  template <class IteratorType>
  static std::map<strings::String, Int> to_map(
      const fct::Range<IteratorType> _range) {
    Int value = 0;

    auto m = std::map<strings::String, Int>();

    for (const auto& key : _range) {
      m[key] = value++;
    }

    return m;
  }

 private:
  /// Counts the document frequency for each individual word.
  template <class IteratorType>
  static std::vector<std::pair<strings::String, size_t>> count_df(
      const fct::Range<IteratorType> _range) {
    const auto process =
        [](const strings::String& _text_field) -> std::set<std::string> {
      return Vocabulary::process_text_field(_text_field);
    };

    const auto range = _range | std::views::transform(process);

    const auto df_map = make_map(range);

    using Pair = std::pair<strings::String, size_t>;

    auto df_vec = std::vector<Pair>(df_map.begin(), df_map.end());

    const auto by_count = [](const Pair& p1, const Pair& p2) -> bool {
      return p1.second > p2.second;
    };

    std::ranges::sort(df_vec, by_count);

    return df_vec;
  }

  /// Counts the document frequency for each individual word.
  template <class RangeType>
  static std::map<strings::String, size_t> make_map(const RangeType& _range) {
    std::map<strings::String, size_t> df_map;

    for (const auto& unique_tokens : _range) {
      for (const auto& token_str : unique_tokens) {
        const auto token = strings::String(token_str);

        const auto it = df_map.find(token);

        if (it == df_map.end()) {
          df_map[token] = 1;
        } else {
          it->second++;
        }
      }
    }

    return df_map;
  }
};

// -------------------------------------------------------------------------
}  // namespace textmining

#endif  // TEXTMINING_VOCABULARY_HPP_
