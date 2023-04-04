// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_DEFINEVARIANT_HPP_
#define FCT_DEFINEVARIANT_HPP_

#include <variant>

namespace fct {

/// Allows you to combine several variants.
template <class... Vars>
struct define_variant;

/// Recursive case - both variants.
template <class... Vars1, class... Vars2, class... Tail>
struct define_variant<std::variant<Vars1...>, std::variant<Vars2...>, Tail...> {
  using type =
      typename define_variant<std::variant<Vars1..., Vars2...>, Tail...>::type;
};

/// Recursive case - variant plus other type.
template <class... Vars, class Head, class... Tail>
struct define_variant<std::variant<Vars...>, Head, Tail...> {
  using type =
      typename define_variant<std::variant<Vars..., Head>, Tail...>::type;
};

/// Recursive case - other type.
template <class Head, class... Tail>
struct define_variant<Head, Tail...> {
  using type = typename define_variant<std::variant<Head>, Tail...>::type;
};

/// Special case - only a single variant is left.
template <class... Vars>
struct define_variant<std::variant<Vars...>> {
  using type = std::variant<Vars...>;
};

template <class... Vars>
using define_variant_t = typename define_variant<Vars...>::type;

}  // namespace fct

#endif  // FCT_DEFINEVARIANT_HPP_
