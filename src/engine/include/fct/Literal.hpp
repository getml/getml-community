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
#include "fct/VisitTree.hpp"
#include "fct/join.hpp"

namespace fct {

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
    static_assert(!duplicate_strings<fields_...>(),
                  "Duplicate strings are not allowed in a Literal.");
  }

  /// Constructs a Literal from another literal.
  Literal(const Literal<fields_...>& _other) : value_(_other.value_) {}

  /// Constructs a Literal from another literal.
  Literal(Literal<fields_...>&& _other) noexcept : value_(_other.value_) {}

  /// A single-field literal is special because it
  /// can also have a default constructor.
  template <ValueType num_fields = num_fields_,
            typename = std::enable_if_t<num_fields == 1>>
  Literal() : value_(0) {}

  ~Literal() = default;

  /// Determines whether the literal contains the string.
  inline static bool contains(const std::string& _str) {
    return has_value<fields_...>(_str);
  }

  /// Determines whether the literal contains the string at compile time.
  template <StringLiteral _name>
  inline static constexpr bool contains() {
    return find_value_of<_name, fields_...>() != -1;
  }

  /// Constructs a new Literal.
  template <StringLiteral _name>
  static Literal<fields_...> make() {
    static_assert(!duplicate_strings<fields_...>(),
                  "Duplicate strings are not allowed in a Literal.");
    return Literal(Literal<fields_...>::template value_of<_name>());
  }

  /// The name defined by the Literal.
  std::string name() const { return find_name<0, fields_...>(); }

  /// Helper function to retrieve a name at compile time.
  template <int _value>
  inline constexpr static auto name_of() {
    static_assert(_value < sizeof...(fields_), "value out of bounds");
    constexpr auto name =
        find_name_of<_value, sizeof...(fields_), fields_...>();
    return Literal<name>();
  }

  /// Assigns from another literal.
  Literal<fields_...> operator=(const Literal<fields_...>& _other) {
    value_ = _other.value_;
    return *this;
  }

  /// Assigns from another literal.
  Literal<fields_...> operator=(Literal<fields_...>&& _other) noexcept {
    value_ = _other.value_;
    return *this;
  }

  /// Assigns the literal from a string
  Literal<fields_...> operator=(const std::string& _str) {
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
    constexpr auto value = find_value_of<_name, fields_...>();
    static_assert(value >= 0, "String not supported.");
    return value;
  }

 private:
  /// Only make is allowed to use this constructor.
  Literal(const ValueType _value) : value_(_value) {
    static_assert(sizeof...(fields_) <= std::numeric_limits<ValueType>::max(),
                  "Too many fields.");
  }

  /// Returns all of the allowed fields.
  template <StringLiteral _head, StringLiteral... _tail>
  inline static std::string allowed_strings(const std::string& _values = "") {
    const auto head = "'" + _head.str() + "'";
    const auto values = _values.size() == 0 ? head : _values + ", " + head;
    if constexpr (sizeof...(_tail) > 0) {
      return allowed_strings<_tail...>(values);
    } else {
      return values;
    }
  }

  /// Whether the Literal contains duplicate strings.
  template <StringLiteral _head, StringLiteral... _tail>
  inline constexpr static bool duplicate_strings() {
    if constexpr (sizeof...(_tail) == 0) {
      return false;
    } else {
      return Literal<_tail...>::template contains<_head>() ||
             duplicate_strings<_tail...>();
    }
  }

  /// Finds the correct index associated with
  /// the string at run time.
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

  /// Finds the correct index associated with
  /// the string at compile time.
  template <int _i, int total, StringLiteral _head, StringLiteral... _tail>
  inline constexpr static auto find_name_of() {
    constexpr int n = total - sizeof...(_tail) - 1;
    if constexpr (n == _i) {
      return _head;
    } else if (n + 1 < total) {
      return find_name_of<_i, total, _tail...>();
    }
  }

  /// Finds the correct value associated with
  /// the string at run time.
  template <StringLiteral _head, StringLiteral... _tail>
  inline static int find_value(const std::string& _str) {
    if (_head.str() == _str) {
      return sizeof...(fields_) - sizeof...(_tail) - 1;
    }
    if constexpr (sizeof...(_tail) == 0) {
      throw std::runtime_error("Literal does not support string '" + _str +
                               "'. The following strings are supported: " +
                               allowed_strings<_head, _tail...>() + ".");
    } else {
      return find_value<_tail...>(_str);
    }
  }

  /// Finds the value of a string literal at compile time.
  template <StringLiteral _name, StringLiteral _head, StringLiteral... _tail>
  static constexpr int find_value_of() {
    if constexpr (_name == _head) {
      return sizeof...(fields_) - sizeof...(_tail) - 1;
    } else if constexpr (sizeof...(_tail) > 0) {
      return find_value_of<_name, _tail...>();
    } else {
      return -1;
    }
  }

  /// Whether the literal contains this string.
  template <StringLiteral _head, StringLiteral... _tail>
  inline static bool has_value(const std::string& _str) {
    if (_head.str() == _str) {
      return true;
    }
    if constexpr (sizeof...(_tail) == 0) {
      return false;
    } else {
      return has_value<_tail...>(_str);
    }
  }

 private:
  /// The underlying value.
  ValueType value_;
};

/// Helper function to retrieve a name at compile time.
template <class LiteralType, int _value>
inline constexpr auto name_of() {
  return LiteralType::template name_of<_value>();
}

/// Helper function to retrieve a value at compile time.
template <class LiteralType, StringLiteral _name>
inline constexpr auto value_of() {
  return LiteralType::template value_of<_name>();
}

}  // namespace fct

#endif  // FCT_LITERAL_HPP_
