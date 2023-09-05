// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_INTERNAL_FROM_NAMED_TUPLE_HPP_
#define RFL_INTERNAL_FROM_NAMED_TUPLE_HPP_

#include <functional>

namespace rfl {
namespace internal {

template <class T, class NamedTupleType>
auto from_named_tuple(const NamedTupleType& _n) {
  const auto make = [](const auto&... _fields) { return T{_fields...}; };
  return std::apply(make, _n.fields());
}

}  // namespace internal
}  // namespace rfl

#endif
