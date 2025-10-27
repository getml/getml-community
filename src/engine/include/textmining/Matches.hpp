// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef TEXTMINING_MATCHES_HPP_
#define TEXTMINING_MATCHES_HPP_

#include "textmining/Int.hpp"
#include "textmining/RowIndex.hpp"

#include <vector>

namespace textmining {
// ----------------------------------------------------------------------------

struct Matches {
  /// Extracts a vector containing all the matches associated with _word.
  template <class MatchType>
  static void extract(
      const Int _word, const RowIndex& _row_index,
      const std::vector<size_t>& _indptr,
      const typename std::vector<MatchType>::iterator _matches_begin,
      const typename std::vector<MatchType>::iterator _matches_end,
      std::vector<MatchType>* _extracted) {
    _extracted->clear();

    const auto range = _row_index.range(_word);

    for (const auto rownum : range) {
      assert_true(rownum < _indptr.size() - 1);

      const auto begin = _matches_begin + _indptr[rownum];

      const auto end = _matches_begin + _indptr[rownum + 1];

      assert_true(begin <= end);

      assert_true(end <= _matches_end);

      for (auto it = begin; it != end; ++it) {
        _extracted->push_back(*it);
      }
    }
  }
};

// ----------------------------------------------------------------------------
}  // namespace textmining

#endif  // TEXTMINING_TEXTMINING_HPP_
