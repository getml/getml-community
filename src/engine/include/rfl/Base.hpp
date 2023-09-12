// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_BASE_HPP_
#define RFL_BASE_HPP_

#include <algorithm>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

namespace rfl {

/// Used to embed another struct into the generated output.
template <class _Type>
struct Base {
  /// The underlying type.
  using Type = std::decay_t<_Type>;

  Base(const Type& _value) : value_(_value) {}

  Base(Type&& _value) : value_(std::forward<Type>(_value)) {}

  Base(const Base<Type>& _base) : value_(_base.get()) {}

  Base(Base<Type>&& _base) : value_(std::forward<Type>(_base.value_)) {}

  template <class T>
  Base(const Base<T>& _base) : value_(_base.get()) {}

  template <class T>
  Base(Base<T>&& _base) : value_(_base.get()) {}

  template <class T, typename std::enable_if<std::is_convertible_v<T, Type>,
                                             bool>::type = true>
  Base(const T& _value) : value_(_value) {}

  template <class T, typename std::enable_if<std::is_convertible_v<T, Type>,
                                             bool>::type = true>
  Base(T&& _value) : value_(_value) {}

  ~Base() = default;

  /// Returns the underlying object.
  inline const Type& get() const { return value_; }

  /// Returns the underlying object.
  inline Type& operator()() { return value_; }

  /// Returns the underlying object.
  inline const Type& operator()() const { return value_; }

  /// Assigns the underlying object.
  inline void operator=(const Type& _value) { value_ = _value; }

  /// Assigns the underlying object.
  inline void operator=(Type&& _value) { value_ = std::forward<Type>(_value); }

  /// Assigns the underlying object.
  template <class T, typename std::enable_if<std::is_convertible_v<T, Type>,
                                             bool>::type = true>
  inline void operator=(const T& _value) {
    value_ = _value;
  }

  /// Assigns the underlying object.
  inline void operator=(const Base<Type>& _base) { value_ = _base.get(); }

  /// Assigns the underlying object.
  inline void operator=(Base<Type>&& _base) {
    value_ = std::forward<Type>(_base);
  }

  /// Assigns the underlying object.
  template <class T>
  inline void operator=(const Base<T>& _base) {
    value_ = _base.get();
  }

  /// Assigns the underlying object.
  template <class T>
  inline void operator=(Base<T>&& _base) {
    value_ = std::forward<T>(_base);
  }

  /// Assigns the underlying object.
  inline void set(const Type& _value) { value_ = _value; }

  /// Assigns the underlying object.
  inline void set(Type&& _value) { value_ = std::forward<Type>(_value); }

  /// The underlying value.
  Type value_;
};

}  // namespace rfl

#endif
