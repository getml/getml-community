// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_EXTRACTDISTRIMINATORS_HPP_
#define FCT_EXTRACTDISTRIMINATORS_HPP_

#include <type_traits>

#include "fct/TaggedUnion.hpp"
#include "fct/define_literal_t.hpp"
#include "fct/field_type.hpp"

namespace fct {

template <class TaggedUnionType>
struct extract_discriminators;

template <StringLiteral _discriminator, class... NamedTupleType>
struct extract_discriminators<TaggedUnion<_discriminator, NamedTupleType...>> {
  using type = define_literal_t<
      std::decay_t<field_type_t<_discriminator, NamedTupleType>>...>;
};

/// Extracts a Literal containing all of the discriminators from a TaggedUnion.
template <class TaggedUnionType>
using extract_discriminators_t =
    typename extract_discriminators<TaggedUnionType>::type;

}  // namespace fct

#endif  // FCT_EXTRACTDISTRIMINATORS_HPP_
