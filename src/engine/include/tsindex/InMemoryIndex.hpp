// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_TSINDEX_INMEMORYINDEX_HPP_
#define CONTAINERS_TSINDEX_INMEMORYINDEX_HPP_

// ----------------------------------------------------------------------------

#include <cstddef>
#include <map>
#include <set>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "fct/fct.hpp"

// ----------------------------------------------------------------------------

#include "tsindex/Float.hpp"
#include "tsindex/IndexParams.hpp"
#include "tsindex/Int.hpp"
#include "tsindex/Key.hpp"

// ----------------------------------------------------------------------------

namespace tsindex {
class InMemoryIndex {
 public:
  InMemoryIndex(const IndexParams& _params);

  ~InMemoryIndex();

 public:
  /// Finds a range of rownums for which .join_key_ == _join_key and
  /// .time_stamp_
  /// <= _time_stamp and time_stamp_ + memory_ > _time_stamp.
  fct::Range<const size_t*> find_range(const Int _join_key,
                                       const Float _time_stamp) const {
    const auto ix_begin = find_ix(_join_key, _time_stamp - memory_);
    const auto ix_end = find_ix(_join_key, _time_stamp);
    return fct::Range<const size_t*>(row_indices_.data() + ix_begin,
                                     row_indices_.data() + ix_end);
  }

 private:
  /// Initializer for the key map.
  static std::map<Key, size_t> make_key_map(
      const IndexParams& _params, const std::vector<size_t>& _row_indices);

  /// Initializer for the row indices.
  static std::vector<size_t> make_row_indices(const IndexParams& _params);

 private:
  /// Finds the upper bound to the index corresponding to the join_key and
  /// time_stamp.
  size_t find_ix(const Int _join_key, const Float _time_stamp) const {
    const auto key = Key{.join_key_ = _join_key, .time_stamp_ = _time_stamp};
    const auto it = key_map_.upper_bound(key);
    return (it == key_map_.end()) ? row_indices_.size() : it->second;
  }

 private:
  /// The difference between the lower_ts and the upper_ts.
  const Float memory_;

  /// Row indices signify the order of the rows in the data frame, when sorted
  /// by the keys.
  const std::vector<size_t> row_indices_;

  /// Maps a key to the first element of row_indices, which is equal to said
  /// key (not in alphabetical order, because row_indices is created before
  /// key_map_).
  const std::map<Key, size_t> key_map_;
};
}  // namespace tsindex

// ----------------------------------------------------------------------------

#endif  // CONTAINERS_TSINDEX_INMEMORY_HPP_
