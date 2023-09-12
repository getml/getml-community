// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_TO_NAMED_TUPLE_HPP_
#define RFL_TO_NAMED_TUPLE_HPP_

#include <iostream>
#include <tuple>

#include "rfl/always_false.hpp"
#include "rfl/internal/has_base_fields.hpp"
#include "rfl/internal/is_named_tuple.hpp"
#include "rfl/internal/to_field_tuple.hpp"
#include "rfl/make_named_tuple.hpp"

namespace rfl {

/// Generates the named tuple that is equivalent to the struct _t.
/// If _t already is a named tuple, then _t will be returned.
/// All fields of the struct must be an rfl::Field.
template <class T>
auto to_named_tuple(const T& _t) {
  if constexpr (internal::is_named_tuple_v<T>) {
    return _t;
  } else {
    auto field_tuple = internal::to_field_tuple(_t);
    using TupleType = std::decay_t<decltype(field_tuple)>;
    if constexpr (!internal::has_base_fields<TupleType>()) {
      const auto ft_to_nt = []<class... Fields>(Fields&&... _fields) {
        return make_named_tuple(std::forward<Fields>(_fields)...);
      };
      return std::apply(ft_to_nt, std::move(field_tuple));
    }
  }
}

}  // namespace rfl

#endif
