// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_DEFINENAMEDTUPLE_HPP_
#define FCT_DEFINENAMEDTUPLE_HPP_

#include "fct/NamedTuple.hpp"

namespace fct {

template <class... FieldTypes>
struct define_named_tuple;

/// Allows you to combine several named tuples and/or additional fields.
/// Recursive case - all types are fields.
template <class Head, class... Tail>
struct define_named_tuple<Head, Tail...> {
  using type = typename define_named_tuple<NamedTuple<Head>, Tail...>::type;
};

/// Allows you to combine several named tuples and/or additional fields.
/// Recursive case - first type is NamedTuple, second type is field.
template <class Head, class... TupContent, class... Tail>
struct define_named_tuple<NamedTuple<TupContent...>, Head, Tail...> {
  using type = typename define_named_tuple<NamedTuple<TupContent..., Head>,
                                           Tail...>::type;
};

/// Allows you to combine several named tuples and/or additional fields.
/// Recursive case - first type is NamedTuple, second type is also NamedTuple.
template <class... TupContent, class... TupContent2, class... Tail>
struct define_named_tuple<NamedTuple<TupContent...>, NamedTuple<TupContent2...>,
                          Tail...> {
  using type =
      typename define_named_tuple<NamedTuple<TupContent..., TupContent2...>,
                                  Tail...>::type;
};

/// Allows you to combine several named tuples and/or additional fields.
template <class... TupContent>
struct define_named_tuple<NamedTuple<TupContent...>> {
  using type = NamedTuple<TupContent...>;
};

template <class... FieldTypes>
using define_named_tuple_t = typename define_named_tuple<FieldTypes...>::type;

}  // namespace fct

#endif  // FCT_DEFINENAMEDTUPLE_HPP_
