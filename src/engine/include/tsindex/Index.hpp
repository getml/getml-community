// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_TSINDEX_INDEX_HPP_
#define CONTAINERS_TSINDEX_INDEX_HPP_

// ----------------------------------------------------------------------------

#include "tsindex/Float.hpp"
#include "tsindex/InMemoryIndex.hpp"
#include "tsindex/Int.hpp"

// ----------------------------------------------------------------------------

namespace tsindex {
class Index {
 public:
  Index(const IndexParams& _params) : impl_(InMemoryIndex(_params)) {}

  ~Index() = default;

 public:
  /// Finds a range of rownums for which .join_key_ == _join_key and
  /// .time_stamp_
  /// <= _time_stamp and time_stamp_ + memory_ > _time_stamp.
  fct::Range<const size_t*> find_range(const Int _join_key,
                                       const Float _time_stamp) const {
    return impl_.find_range(_join_key, _time_stamp);
  }

 private:
  /// Implements the index functionality
  const InMemoryIndex impl_;
};
}  // namespace tsindex

// ----------------------------------------------------------------------------

#endif  // CONTAINERS_TSINDEX_INMEMORY_HPP_
