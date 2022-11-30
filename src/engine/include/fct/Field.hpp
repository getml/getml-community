// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_FIELD_HPP_
#define FCT_FIELD_HPP_

#include <algorithm>
#include <string_view>
#include <tuple>

#include "fct/StringLiteral.hpp"

namespace fct {

/// Used to define a field in the NamedTuple.
template <StringLiteral _name, class _Type>
struct Field {
  Field(const Type& _value) : value_(_value) {}

  ~Field() = default;

  using Type = _Type;

  /// The name of the field.
  constexpr static const StringLiteral name_ = _name;

  /// The underlying value.
  const Type value_;
};

}  // namespace fct

#endif  // FCT_FIELD_HPP_
