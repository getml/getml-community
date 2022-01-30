#ifndef STL_ACCESSITERATOR_HPP_
#define STL_ACCESSITERATOR_HPP_

// -------------------------------------------------------------------------

#include <iterator>

// -------------------------------------------------------------------------

#include "debug/debug.hpp"

// -------------------------------------------------------------------------

namespace stl {

/// Implements an iterator for any container that supports
/// operator[].
template <class T, class ContainerType>
class AccessIterator {
 public:
  using iterator_category = std::input_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = T;
  using pointer = value_type*;
  using reference = value_type&;

  /// Default constructor.
  AccessIterator() : i_(0), ptr_(nullptr) {}

  /// Construct by value.
  explicit AccessIterator(const size_t _i, const ContainerType* _ptr)
      : i_(_i), ptr_(_ptr) {}

  /// Destructor.
  ~AccessIterator() = default;

  ///
  inline value_type operator*() const {
    assert_true(ptr_);
    return (*ptr_)[i_];
  }

  /// Prefix incrementor
  inline AccessIterator<T, ContainerType>& operator++() {
    ++i_;
    return *this;
  }

  /// Postfix incrementor.
  inline AccessIterator<T, ContainerType> operator++(int) {
    AccessIterator<T, ContainerType> tmp = *this;
    ++(*this);
    return tmp;
  }

  /// Addition operator
  inline AccessIterator<T, ContainerType>& operator+=(const size_t _j) {
    i_ += _j;
    return *this;
  }

  /// Substraction operator
  inline AccessIterator<T, ContainerType>& operator-=(const size_t _j) {
    i_ -= _j;
    return *this;
  }

  /// Check equality.
  friend inline bool operator==(const AccessIterator<T, ContainerType>& _a,
                                const AccessIterator<T, ContainerType>& _b) {
    assert_true(_a.ptr_ == _b.ptr_);
    return _a.i_ == _b.i_;
  }

  /// Check inequality.
  friend inline bool operator!=(const AccessIterator<T, ContainerType>& _a,
                                const AccessIterator<T, ContainerType>& _b) {
    assert_true(_a.ptr_ == _b.ptr_);
    return _a.i_ != _b.i_;
  }

  /// Addition operator.
  friend inline AccessIterator<T, ContainerType> operator+(
      const AccessIterator<T, ContainerType>& _a, const size_t _j) {
    AccessIterator<T, ContainerType> tmp = _a;
    tmp += _j;
    return tmp;
  }

  /// Substraction operator.
  friend inline AccessIterator<T, ContainerType> operator-(
      const AccessIterator<T, ContainerType>& _a, const size_t _j) {
    AccessIterator<T, ContainerType> tmp = _a;
    tmp -= _j;
    return tmp;
  }

 private:
  /// The current index.
  size_t i_;

  /// A pointer to the underlying container.
  const ContainerType* ptr_;
};

// -------------------------------------------------------------------------
}  // namespace stl

#endif  // STL_IOTAITERATOR_HPP_
