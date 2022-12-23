// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_LITERAL_HPP_
#define FCT_LITERAL_HPP_

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "debug/debug.hpp"
#include "fct/StringLiteral.hpp"

namespace fct {

/// Helper class to retrieve the value at compile time.
template <StringLiteral name_, StringLiteral... fields_>
struct GetValueOf {
 public:
  /// Retrieves the value.
  static constexpr auto get() { return find_value_of<fields_...>(); }

 private:
  /// Finds the value of a string literal at compile time.
  template <StringLiteral _head, StringLiteral... _tail>
  static constexpr auto find_value_of() {
    if constexpr (name_ == _head) {
      return sizeof...(fields_) - sizeof...(_tail) - 1;
    } else {
      static_assert(sizeof...(_tail) > 0, "Field name not supported.");
      return find_value_of<_tail...>();
    }
  }
};

template <StringLiteral... fields_>
class Literal {
 public:
  using ValueType =
      typename std::conditional<sizeof...(fields_) <=
                                    std::numeric_limits<std::uint8_t>::max(),
                                std::uint8_t, std::uint16_t>::type;

  /// Constructs a Literal from a string.
  Literal(const std::string& _str) : value_(find_value<fields_...>(_str)) {
    static_assert(sizeof...(fields_) <= std::numeric_limits<ValueType>::max(),
                  "Too many fields.");
  }

  ~Literal() = default;

  /// Constructs a new Literal.
  template <StringLiteral _name>
  static Literal<fields_...> make() {
    return Literal(GetValueOf<_name, fields_...>::get());
  }

  /// The name defined by the Literal.
  std::string name() const { return find_name<0, fields_...>(); }

  /// Assigns the literal from a string
  Literal operator=(const std::string& _str) {
    value_ = find_value<fields_...>(_str);
    return *this;
  }

  /// Equality operator other Literals.
  inline bool operator==(const Literal<fields_...>& _other) const {
    return value() == _other.value();
  }

  /// Inequality operator for other Literals.
  inline bool operator!=(const Literal<fields_...>& _other) const {
    return value() != _other.value();
  }

  /// Equality operator for strings.
  inline bool operator==(const std::string& _str) const {
    return name() == _str;
  }

  /// Inequality operator for strings.
  inline bool operator!=(const std::string& _str) const {
    return name() != _str;
  }

  /// Returns the value actually contained in the Literal.
  inline ValueType value() const { return value_; }

  /// Returns the value of the string literal in the template.
  template <StringLiteral _name>
  static constexpr ValueType value_of() {
    return GetValueOf<_name, fields_...>::get();
  }

 private:
  /// Only make is allowed to use this constructor.
  Literal(const ValueType _value) : value_(_value) {
    static_assert(sizeof...(fields_) <= std::numeric_limits<ValueType>::max(),
                  "Too many fields.");
  }

  /// Finds the correct index associated with
  /// the string.
  template <ValueType _i, StringLiteral _head, StringLiteral... _tail>
  std::string find_name() const {
    if (_i == value_) {
      return _head.str();
    }
    if constexpr (sizeof...(_tail) == 0) {
      assert_true(false);
      return "";
    } else {
      return find_name<_i + 1, _tail...>();
    }
  }

  /// Finds the correct value associated with
  /// the string at run time.
  template <StringLiteral _head, StringLiteral... _tail>
  static int find_value(const std::string& _str) {
    if (_head.str() == _str) {
      return sizeof...(fields_) - sizeof...(_tail) - 1;
    }
    if constexpr (sizeof...(_tail) == 0) {
      throw std::runtime_error("Literal does not support string '" + _str +
                               "'.");
    } else {
      return find_value<_tail...>(_str);
    }
  }

 private:
  /// The underlying value.
  ValueType value_;
};

/// Helper class to retrieve a value from a literal.
template <class LiteralType, StringLiteral name_>
struct GetValueOfLiteral;

/// Helper class to retrieve a value from a literal.
template <StringLiteral... fields_, StringLiteral name_>
struct GetValueOfLiteral<Literal<fields_...>, name_> {
  static constexpr auto get() { return GetValueOf<name_, fields_...>::get(); }
};

/// Helper function to retrieve a value at compile time.
template <class LiteralType, StringLiteral _name>
inline constexpr auto value_of() {
  return GetValueOfLiteral<LiteralType, _name>::get();
}

}  // namespace fct
#endif  // FCT_LITERAL`_HPP_
