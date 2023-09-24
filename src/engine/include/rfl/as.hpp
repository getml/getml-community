// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_AS_HPP_
#define RFL_AS_HPP_

#include "rfl/from_named_tuple.hpp"
#include "rfl/make_named_tuple.hpp"
#include "rfl/to_named_tuple.hpp"

namespace rfl {

/// Generates a type T from the input values.
template <class T, class Head, class... Tail>
T as(Head&& _head, Tail&&... _tail) {
  if constexpr (sizeof...(_tail) == 0) {
    return from_named_tuple<T>(to_named_tuple(std::forward<Head>(_head)));
  } else {
    return from_named_tuple<T>(
        to_named_tuple(std::forward<Head>(_head))
            .add(to_named_tuple(std::forward<Tail>(_tail))...));
  }
}

/// Generates a type T from the input values.
template <class T, class Head, class... Tail>
T as(const Head& _head, const Tail&... _tail) {
  if constexpr (sizeof...(_tail) == 0) {
    return from_named_tuple<T>(to_named_tuple(_head));
  } else {
    return from_named_tuple<T>(
        to_named_tuple(_head).add(to_named_tuple(_tail)...));
  }
}

}  // namespace rfl

#endif
