// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_FIELD_HPP_
#define RFL_FIELD_HPP_

#include <algorithm>
#include <string_view>
#include <tuple>
#include <utility>

#include "rfl/internal/StringLiteral.hpp"

namespace rfl {

/// Used to define a field in the NamedTuple.
template <internal::StringLiteral _name, class _Type>
struct Field {
  Field(const _Type& _value) : value_(_value) {}

  Field(_Type&& _value) : value_(std::forward<Type>(_value)) {}

  ~Field() = default;

  using Type = _Type;

  /// The name of the field.
  constexpr static const internal::StringLiteral name_ = _name;

  /// Returns the underlying object.
  inline const Type& get() const { return value_; }

  /// Returns the underlying object.
  inline const Type& operator*() const { return value_; }

  /// Returns the underlying object.
  inline const Type& operator->() const { return value_; }

  /// The underlying value.
  const Type value_;
};

template <internal::StringLiteral _name, class _Type>
inline auto make_field(const _Type& _value) {
  return Field<_name, _Type>(_value);
}

}  // namespace rfl

#endif  // RFL_FIELD_HPP_
