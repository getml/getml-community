// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_TAGGEDUNION_HPP_
#define FCT_TAGGEDUNION_HPP_

#include <variant>

#include "fct/StringLiteral.hpp"

namespace fct {

template <StringLiteral _discriminator, class... NamedTupleTypes>
struct TaggedUnion {
  static constexpr StringLiteral discrimininator_ = _discriminator;

  using VariantType = std::variant<NamedTupleTypes...>;

  /// The underlying variant - a TaggedUnion is a thin wrapper
  /// around a variant that is mainly used for parsing.
  VariantType variant_;
};

/// Implements the visitor pattern for Literals.
template <class Visitor, StringLiteral _discriminator, class... Args>
inline auto visit(const Visitor& _visitor,
                  const TaggedUnion<_discriminator, Args...> _tagged_union) {
  return std::visit(_visitor, _tagged_union.variant_);
}

}  // namespace fct

#endif  // FCT_TAGGEDUNION_HPP_
