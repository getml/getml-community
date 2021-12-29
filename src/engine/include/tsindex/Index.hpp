#ifndef ENGINE_CONTAINERS_TSINDEX_INDEX_HPP_
#define ENGINE_CONTAINERS_TSINDEX_INDEX_HPP_

// ----------------------------------------------------------------------------

#include "stl/stl.hpp"

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
  stl::Range<const size_t*> find_range(const Int _join_key,
                                       const Float _time_stamp) const {
    return impl_.find_range(_join_key, _time_stamp);
  }

 private:
  /// Implements the index functionality
  const InMemoryIndex impl_;
};
}  // namespace tsindex

// ----------------------------------------------------------------------------

#endif  // ENGINE_CONTAINERS_TSINDEX_INMEMORY_HPP_
