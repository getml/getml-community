// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_TAGGEDUNION_HPP_
#define RFL_TAGGEDUNION_HPP_

#include <variant>

#include "rfl/internal/StringLiteral.hpp"

namespace rfl {

template <internal::StringLiteral _discriminator, class... NamedTupleTypes>
struct TaggedUnion {
  static constexpr internal::StringLiteral discrimininator_ = _discriminator;

  /// The type of the underlying variant.
  using VariantType = std::variant<NamedTupleTypes...>;

  /// The underlying variant - a TaggedUnion is a thin wrapper
  /// around a variant that is mainly used for parsing.
  VariantType variant_;
};

}  // namespace rfl

#endif  // RFL_TAGGEDUNION_HPP_
