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

/// Finds the correct index associated with
/// the string.
template <int _i, int total, StringLiteral _head, StringLiteral... _tail>
inline constexpr auto find_name_of() {
  constexpr int n = total - sizeof...(_tail) - 1;
  if constexpr (n == _i) {
    return _head;
  } else if (n + 1 < total) {
    return find_name_of<_i, total, _tail...>();
  }
}

template <StringLiteral... fields_>
class Literal {
 public:
  using ValueType =
      std::conditional_t<sizeof...(fields_) <=
                             std::numeric_limits<std::uint8_t>::max(),
                         std::uint8_t, std::uint16_t>;

  /// The number of different fields or different options that the literal
  /// can assume.
  static constexpr ValueType num_fields_ = sizeof...(fields_);

  /// Constructs a Literal from a string.
  Literal(const std::string& _str) : value_(find_value<fields_...>(_str)) {
    static_assert(sizeof...(fields_) > 0,
                  "There must be at least one field in a Literal.");
    static_assert(sizeof...(fields_) <= std::numeric_limits<ValueType>::max(),
                  "Too many fields.");
  }

  /// A single-field literal is special because it
  /// can also have a default constructor.
  Literal() : value_(0) {
    static_assert(
        num_fields_ == 1,
        "Only Literals with a single field can use the default constructor.");
  }

  ~Literal() = default;

  /// Constructs a new Literal.
  template <StringLiteral _name>
  static Literal<fields_...> make() {
    return Literal(GetValueOf<_name, fields_...>::get());
  }

  /// The name defined by the Literal.
  std::string name() const { return find_name<0, fields_...>(); }

  /// Helper function to retrieve a name at compile time.
  template <int _value>
  inline constexpr static auto name_of() {
    constexpr auto name =
        find_name_of<_value, sizeof...(fields_), fields_...>();
    return Literal<name>();
  }

  /// Assigns the literal from a string
  Literal operator=(const std::string& _str) {
    value_ = find_value<fields_...>(_str);
    return *this;
  }

  /// Equality operator other Literals.
  inline bool operator==(const Literal<fields_...>& _other) const {
    return value() == _other.value();
  }

  /// Equality operator other Literals with different fields.
  template <StringLiteral... other_fields>
  inline bool operator==(const Literal<other_fields...>& _other) const {
    return name() == _other.name();
  }

  /// Inequality operator for other Literals.
  template <StringLiteral... other_fields>
  inline bool operator!=(const Literal<other_fields...>& _other) const {
    return !(*this == _other);
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

/// Helper class to retrieve a name from a literal.
template <class LiteralType, int value_>
struct GetNameOfLiteral;

/// Helper class to retrieve a name from a literal.
template <StringLiteral... fields_, int value_>
struct GetNameOfLiteral<Literal<fields_...>, value_> {
  static constexpr auto name_ =
      find_name_of<value_, sizeof...(fields_), fields_...>();
};

/// Helper function to retrieve a name at compile time.
template <class LiteralType, int _value>
inline constexpr auto name_of() {
  constexpr auto name = GetNameOfLiteral<LiteralType, _value>::name_;
  return Literal<name>();
}

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

/// Implements the visitor pattern for Literals.
template <class Visitor, int i = 0, StringLiteral... _fields, class... Args>
inline auto visit(const Visitor& _visitor, const Literal<_fields...> _literal,
                  Args&&... _args) {
  constexpr typename Literal<_fields...>::ValueType value = i;
  if (_literal.value() == value) {
    return _visitor(name_of<Literal<_fields...>, i>());
  }
  if constexpr (i + 1 < sizeof...(_fields)) {
    return visit<Visitor, i + 1, _fields...>(_visitor, _literal, _args...);
  }
}

}  // namespace fct
#endif  // FCT_LITERAL`_HPP_
