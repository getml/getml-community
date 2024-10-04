// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "textmining/RowIndex.hpp"

#include <numeric>

namespace textmining {

RowIndex::RowIndex(const WordIndex& _word_index)
    : vocabulary_(_word_index.vocabulary_ptr()) {
  std::tie(indptr_, rownums_) = make_indptr_and_rownums(_word_index);
}

// ----------------------------------------------------------------------------

RowIndex::~RowIndex() = default;

// ----------------------------------------------------------------------------

std::pair<std::vector<size_t>, std::vector<size_t>>
RowIndex::make_indptr_and_rownums(const WordIndex& _word_index) const {
  auto indptr = std::vector<size_t>(vocabulary().size() + 1);

  for (const auto& word_ix : _word_index.words()) {
    assert_true(static_cast<size_t>(word_ix) < indptr.size() - 1);
    ++indptr[word_ix + 1];
  }

  std::partial_sum(indptr.begin(), indptr.end(), indptr.begin());

  auto counts = std::vector<size_t>(vocabulary().size());

  auto rownums = std::vector<size_t>(_word_index.words().size());

  for (size_t rownum = 0; rownum < _word_index.nrows(); ++rownum) {
    for (const auto& word_ix : _word_index.range(rownum)) {
      assert_true(static_cast<size_t>(word_ix) < indptr.size() - 1);

      assert_true(indptr[word_ix] + counts[word_ix] < indptr[word_ix + 1]);

      assert_true(indptr[word_ix] + counts[word_ix] < rownums.size());

      rownums[indptr[word_ix] + counts[word_ix]] = rownum;

      ++counts[word_ix];
    }
  }

  return std::make_pair(indptr, rownums);
}

}  // namespace textmining
