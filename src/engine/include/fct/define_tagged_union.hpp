// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_DEFINETAGGEDUNION_HPP_
#define FCT_DEFINETAGGEDUNION_HPP_

#include "fct/StringLiteral.hpp"
#include "fct/TaggedUnion.hpp"

namespace fct {

/// Allows you to combine several tagged unions.
template <StringLiteral _discriminator, class... TaggedUnionTypes>
struct define_tagged_union;

/// Recursive case - both tagged union.
template <StringLiteral _discriminator, class... NamedTupleTypes1,
          class... NamedTupleTypes2, class... Tail>
struct define_tagged_union<
    _discriminator, TaggedUnion<_discriminator, NamedTupleTypes1...>,
    TaggedUnion<_discriminator, NamedTupleTypes2...>, Tail...> {
  using type = typename define_tagged_union<
      _discriminator,
      TaggedUnion<_discriminator, NamedTupleTypes1..., NamedTupleTypes2...>,
      Tail...>::type;
};

/// Recursive case - tagged union plus named tuple.
template <StringLiteral _discriminator, class... NamedTupleTypes,
          class... FieldTypes, class... Tail>
struct define_tagged_union<_discriminator,
                           TaggedUnion<_discriminator, NamedTupleTypes...>,
                           NamedTuple<FieldTypes...>, Tail...> {
  using type = typename define_tagged_union<
      _discriminator,
      TaggedUnion<_discriminator, NamedTupleTypes...,
                  NamedTuple<FieldTypes...>>,
      Tail...>::type;
};

/// Recursive case - named tuple.
template <StringLiteral _discriminator, class... FieldTypes, class... Tail>
struct define_tagged_union<_discriminator, NamedTuple<FieldTypes...>, Tail...> {
  using type = typename define_tagged_union<
      _discriminator, TaggedUnion<_discriminator, NamedTuple<FieldTypes...>>,
      Tail...>::type;
};

/// Special case - only a single TaggedUnion is left.
template <StringLiteral _discriminator, class... NamedTupleTypes>
struct define_tagged_union<_discriminator,
                           TaggedUnion<_discriminator, NamedTupleTypes...>> {
  using type = TaggedUnion<_discriminator, NamedTupleTypes...>;
};

template <StringLiteral _discriminator, class... TaggedUnionTypes>
using define_tagged_union_t =
    typename define_tagged_union<_discriminator, TaggedUnionTypes...>::type;

}  // namespace fct

#endif  // FCT_DEFINETAGGEDUNION_HPP_
