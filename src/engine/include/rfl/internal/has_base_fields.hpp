// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_INTERNAL_HASBASEFIELDS_HPP_
#define RFL_INTERNAL_HASBASEFIELDS_HPP_

#include <tuple>
#include <type_traits>
#include <utility>

#include "rfl/internal/is_base_field.hpp"

namespace rfl {
namespace internal {

template <class TupleType, int _i = 0>
constexpr bool has_base_fields() {
  if constexpr (_i == std::tuple_size_v<TupleType>) {
    return false;
  } else {
    using T = std::tuple_element_t<_i, TupleType>;
    return is_base_field<T>::value || has_base_fields<TupleType, _i + 1>();
  }
}

}  // namespace internal
}  // namespace rfl

#endif
