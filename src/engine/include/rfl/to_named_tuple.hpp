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

template <class FieldTuple, class... Args>
auto flatten_field_tuple(FieldTuple&& _t, Args&&... _args) {
  constexpr auto i = sizeof...(Args);
  if constexpr (i == std::tuple_size_v<FieldTuple>) {
    return std::tuple_cat(std::forward<Args>(_args)...);
  } else {
    using T = std::tuple_element_t<i, FieldTuple>;
    if constexpr (internal::is_base_field<T>::value) {
      return flatten_field_tuple(_t, std::forward<Args>(_args)...,
                                 flatten_field_tuple(internal::to_field_tuple(
                                     std::move(std::get<i>(_t).get()))));
    } else {
      return flatten_field_tuple(_t, std::forward<Args>(_args)...,
                                 std::make_tuple(std::move(std::get<i>(_t))));
    }
  }
}

/// Generates the named tuple that is equivalent to the struct _t.
/// If _t already is a named tuple, then _t will be returned.
/// All fields of the struct must be an rfl::Field.
template <class T>
auto to_named_tuple(const T& _t) {
  if constexpr (internal::is_named_tuple_v<T>) {
    return _t;
  } else {
    auto field_tuple = internal::to_field_tuple(_t);

    using FieldTuple = std::decay_t<decltype(field_tuple)>;

    const auto ft_to_nt = []<class... Fields>(Fields&&... _fields) {
      return make_named_tuple(std::forward<Fields>(_fields)...);
    };

    if constexpr (!internal::has_base_fields<FieldTuple>()) {
      return std::apply(ft_to_nt, std::move(field_tuple));
    } else {
      auto flattened_tuple = flatten_field_tuple(std::move(field_tuple));
      return std::apply(ft_to_nt, std::move(flattened_tuple));
    }
  }
}

}  // namespace rfl

#endif
