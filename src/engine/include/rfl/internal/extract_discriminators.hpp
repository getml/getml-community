// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_INTERNAL_EXTRACTDISTRIMINATORS_HPP_
#define RFL_INTERNAL_EXTRACTDISTRIMINATORS_HPP_

#include <type_traits>

#include "rfl/TaggedUnion.hpp"
#include "rfl/define_literal.hpp"
#include "rfl/field_type.hpp"

namespace rfl {
namespace internal {

template <class TaggedUnionType>
struct extract_discriminators;

template <StringLiteral _discriminator, class... NamedTupleType>
struct extract_discriminators<TaggedUnion<_discriminator, NamedTupleType...>> {
  using type = define_literal_t<
      std::decay_t<field_type_t<_discriminator, NamedTupleType>>...>;
};

}  // namespace internal
}  // namespace rfl

#endif
