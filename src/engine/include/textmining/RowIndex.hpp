#ifndef TEXTMINING_ROWINDEX_HPP_
#define TEXTMINING_ROWINDEX_HPP_

// -------------------------------------------------------------------------

#include <memory>
#include <utility>
#include <vector>

// -------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "stl/stl.hpp"
#include "strings/strings.hpp"

// -------------------------------------------------------------------------

#include "textmining/Float.hpp"
#include "textmining/Int.hpp"
#include "textmining/WordIndex.hpp"

// -------------------------------------------------------------------------

namespace textmining {

class RowIndex {
 public:
  RowIndex(const WordIndex& _word_index);

  ~RowIndex();

 public:
  /// Returns the range for the _i'th word in the vocabulary
  stl::Range<const size_t*> range(const Int _i) const {
    assert_true(_i >= 0);
    assert_true(static_cast<size_t>(_i + 1) < indptr_.size());
    assert_true(indptr_[_i + 1] <= rownums_.size());
    return stl::Range(rownums_.data() + indptr_[_i],
                      rownums_.data() + indptr_[_i + 1]);
  }

  /// The size of the vocabulary.
  const size_t size() const { return vocabulary().size(); }

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

 private:
  /// Generates the index and the indptr during construction.
  std::pair<std::vector<size_t>, std::vector<size_t>> make_indptr_and_rownums(
      const WordIndex& _word_index) const;

 private:
  /// Indicates the beginning and of each word in rownums.
  std::vector<size_t> indptr_;

  /// Indicates the row numbers of the text field.
  std::vector<size_t> rownums_;

  /// The vocabulary.
  std::shared_ptr<const std::vector<strings::String>> vocabulary_;
};

// -------------------------------------------------------------------------
}  // namespace textmining

#endif  // TEXTMINING_ROWINDEX_HPP_
