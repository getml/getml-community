// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_COLUMNVIEWITERATOR_HPP_
#define CONTAINERS_COLUMNVIEWITERATOR_HPP_

#include <functional>
#include <iterator>
#include <optional>
#include <utility>

namespace containers {

template <class T>
class ColumnViewIterator {
 public:
  using iterator_category = std::input_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = T;
  using pointer = value_type*;
  using reference = value_type&;

  typedef std::function<std::optional<T>(size_t)> ValueFunc;

  /// Iterator to the beginning.
  explicit ColumnViewIterator(const ValueFunc _value_func)
      : i_(0), value_(_value_func(0)), value_func_(_value_func) {}

  /// Iterator to the end.
  ColumnViewIterator()
      : i_(0), value_(std::nullopt), value_func_(std::nullopt) {}

  /// Copy constructor.
  ColumnViewIterator(const ColumnViewIterator<T>& _other)
      : i_(_other.i_), value_(_other.value_), value_func_(_other.value_func_) {}

  /// Move constructor.
  ColumnViewIterator(ColumnViewIterator<T>&& _other) noexcept
      : i_(_other.i_),
        value_(std::move(_other.value_)),
        value_func_(std::move(_other.value_func_)) {}

  /// Destructor.
  ~ColumnViewIterator() = default;

  /// Copy assignment operator.
  ColumnViewIterator<T>& operator=(const ColumnViewIterator<T>& _other) {
    i_ = _other.i_;
    value_ = _other.value_;
    value_func_ = _other.value_func_;
    return *this;
  }

  /// Move assignment operator.
  ColumnViewIterator<T>& operator=(ColumnViewIterator<T>&& _other) noexcept {
    i_ = _other.i_;
    value_ = std::move(_other.value_);
    value_func_ = std::move(_other.value_func_);
    return *this;
  }

  /// Deference operator
  inline value_type operator*() const {
    assert_true(value_);
    return *value_;
  }

  /// Deference operator
  inline value_type operator->() const { return **this; }

  /// Prefix incrementor
  inline ColumnViewIterator<T>& operator++() {
    ++i_;
    if (value_func_) {
      value_ = (*value_func_)(i_);
    }
    return *this;
  }

  /// Postfix incrementor.
  inline ColumnViewIterator<T> operator++(int) {
    ColumnViewIterator<T> tmp = *this;
    ++(*this);
    return tmp;
  }

  /// Check equality
  friend inline bool operator==(const ColumnViewIterator<T>& _a,
                                const ColumnViewIterator<T>& _b) {
    return (!_a.value_ && !_b.value_) ||
           (_a.value_ && _b.value_ && _a.i_ == _b.i_);
  }

  /// Inequality operator.
  friend inline bool operator!=(const ColumnViewIterator<T>& _a,
                                const ColumnViewIterator<T>& _b) {
    return !(_a == _b);
  }

 private:
  /// The current index.
  size_t i_;

  /// The current value.
  std::optional<T> value_;

  /// The function returning the actual data point.
  std::optional<ValueFunc> value_func_;
};

}  // namespace containers

#endif  // CONTAINERS_COLUMNVIEWITERATOR_HPP_
