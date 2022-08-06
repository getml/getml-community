// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef BINNING_WORDMAKER_HPP_
#define BINNING_WORDMAKER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <numeric>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "fct/fct.hpp"
#include "multithreading/multithreading.hpp"
#include "strings/strings.hpp"

// ----------------------------------------------------------------------------

#include "binning/Float.hpp"
#include "binning/Int.hpp"

// ----------------------------------------------------------------------------

namespace binning {
// ----------------------------------------------------------------------------

template <class MatchType, class GetRangeType>
struct WordMaker {
 public:
  /// Generates a list of all words included in this set of the matches.
  static std::shared_ptr<const std::vector<Int>> make_words(
      const std::vector<strings::String>& _vocabulary,
      const GetRangeType& _get_range,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _end,
      multithreading::Communicator* _comm);
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class MatchType, class GetRangeType>
std::shared_ptr<const std::vector<Int>>
WordMaker<MatchType, GetRangeType>::make_words(
    const std::vector<strings::String>& _vocabulary,
    const GetRangeType& _get_range,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _end,
    multithreading::Communicator* _comm) {
  // ------------------------------------------------------------------------
  // Find unique words (signified by a boolean vector). We cannot use the
  // actual boolean type, because bool is smaller than char and therefore the
  // all_reduce operator won't work. So we use std::int8_t instead.

  auto included = std::vector<std::int8_t>(_vocabulary.size(), 0);

  for (auto it = _begin; it != _end; ++it) {
    const auto range = _get_range(*it);

    for (const auto word_ix : range) {
      assert_true(word_ix >= 0);
      assert_true(static_cast<size_t>(word_ix) < included.size());

      included[word_ix] = 1;
    }
  }

  // ------------------------------------------------------------------------

  multithreading::Reducer::reduce(multithreading::maximum<std::int8_t>(),
                                  &included, _comm);

  // ------------------------------------------------------------------------

  const auto is_included = [included](Int i) -> bool {
    return included[i] == 1;
  };

  const auto iota = fct::iota<Int>(0, included.size());

  auto range = iota | VIEWS::filter(is_included);

  // ------------------------------------------------------------------------

  return std::make_shared<const std::vector<Int>>(
      fct::collect::vector<Int>(range));

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace binning

#endif  // BINNING_WORDMAKER_HPP_
