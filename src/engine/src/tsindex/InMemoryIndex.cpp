#include "tsindex/InMemoryIndex.hpp"

// ----------------------------------------------------------------------------

#include <algorithm>
#include <cmath>
#include <limits>

// ----------------------------------------------------------------------------

namespace tsindex {

// ----------------------------------------------------------------------------

InMemoryIndex::InMemoryIndex(const IndexParams& _params)
    : memory_(_params.memory_),
      row_indices_(make_row_indices(_params)),
      key_map_(make_key_map(_params, row_indices_)) {}

// ----------------------------------------------------------------------------

InMemoryIndex::~InMemoryIndex() = default;

// ----------------------------------------------------------------------------

std::map<Key, size_t> InMemoryIndex::make_key_map(
    const IndexParams& _params, const std::vector<size_t>& _row_indices) {
  assert_true(_params.join_keys_.end() >= _params.join_keys_.begin());

  const auto size = static_cast<size_t>(_params.join_keys_.end() -
                                        _params.join_keys_.begin());

  assert_true(static_cast<size_t>(_params.lower_ts_.end() -
                                  _params.lower_ts_.begin()) == size);

  const auto make_key = [&_params](const size_t _i) -> Key {
    return Key{.join_key_ = *(_params.join_keys_.begin() + _i),
               .time_stamp_ = *(_params.lower_ts_.begin() + _i)};
  };

  std::map<Key, size_t> key_map;

  for (size_t i = 0; i < _row_indices.size(); ++i) {
    const auto ix = _row_indices[i];
    const auto key = make_key(ix);
    if (key_map.find(key) == key_map.end()) {
      key_map[key] = i;
    }
  }

  return key_map;
}

// ----------------------------------------------------------------------------

std::vector<size_t> InMemoryIndex::make_row_indices(
    const IndexParams& _params) {
  assert_true(_params.join_keys_.end() >= _params.join_keys_.begin());

  const auto size = static_cast<size_t>(_params.join_keys_.end() -
                                        _params.join_keys_.begin());

  assert_true(static_cast<size_t>(_params.lower_ts_.end() -
                                  _params.lower_ts_.begin()) == size);

  const auto make_key = [&_params](const size_t _i) -> Key {
    return Key{.join_key_ = *(_params.join_keys_.begin() + _i),
               .time_stamp_ = *(_params.lower_ts_.begin() + _i)};
  };

  const auto comp = [&make_key](const size_t _i, const size_t _j) -> bool {
    return make_key(_i) < make_key(_j);
  };

  assert_true(_params.rownums_);

  auto row_indices = *_params.rownums_;

  std::stable_sort(row_indices.begin(), row_indices.end(), comp);

  return row_indices;
}

// ----------------------------------------------------------------------------

}  // namespace tsindex
