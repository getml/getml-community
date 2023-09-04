// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_INTERNAL_HELPER_TUPLE_HPP_
#define RFL_INTERNAL_HELPER_TUPLE_HPP_

#include <type_traits>

#include "rfl/internal/to_tuple.hpp"

namespace rfl {
namespace internal {

/// Simple wrapper that is meant to instruct the Parser, because we want this
/// treated differently than normal tuples.
template <class _TupleType>
struct HelperTuple {
  using TupleType = _TupleType;
  TupleType tup_;
};

template <class T>
auto to_helper_tuple(const T& _t) {
  const auto tup = to_tuple(_t);
  return HelperTuple<std::decay_t<decltype(tup)>>{std::move(tup)};
}

}  // namespace internal
}  // namespace rfl

#endif
