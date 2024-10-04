// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef MEMMAP_VECTOR_HPP_
#define MEMMAP_VECTOR_HPP_

#include <cstddef>
#include <memory>
#include <utility>

#include "debug/debug.hpp"
#include "memmap/Pool.hpp"
#include "memmap/VectorImpl.hpp"

namespace memmap {

template <class T>
class Vector {
 public:
  using iterator = typename VectorImpl<T>::iterator;
  using const_iterator = typename VectorImpl<T>::const_iterator;

 public:
  explicit Vector(const std::shared_ptr<Pool> &_pool)
      : impl_(VectorImpl<T>(VectorImpl<T>::NOT_ALLOCATED,
                            _pool ? _pool.get() : nullptr, 0)),
        pool_(_pool) {
    assert_true(pool_);
    const auto new_capacity = sizeof(T) < pool()->page_size()
                                  ? pool()->page_size() / sizeof(T)
                                  : static_cast<size_t>(1);
    allocate(new_capacity);
  }

  template <class IteratorType>
  Vector(const std::shared_ptr<Pool> &_pool, IteratorType _begin,
         IteratorType _end)
      : Vector(_pool) {
    for (auto it = _begin; it != _end; ++it) {
      push_back(*it);
    }
  }

  Vector(const std::shared_ptr<Pool> &_pool, const size_t _size)
      : Vector(_pool) {
    for (size_t i = 0; i < _size; ++i) {
      push_back(T());
    }
  }

  /// Move constructor
  Vector(Vector<T> &&_other) noexcept { move(&_other); }

  Vector(const Vector<T> &_other) = delete;

  ~Vector() { deallocate(); }

 public:
  /// Move assignment operator.
  Vector<T> &operator=(Vector<T> &&_other) noexcept;

 public:
  /// Access operator with bound checks
  T &at(size_t _i) {
    throw_unless(_i < impl_.size(),
                 "Out of bounds. _i: " + std::to_string(_i) +
                     ", size_: " + std::to_string(impl_.size()));
    return impl_[_i];
  }

  /// Access operator with bound checks
  T at(size_t _i) const {
    throw_unless(_i < impl_.size(),
                 "Out of bounds. _i: " + std::to_string(_i) +
                     ", size_: " + std::to_string(impl_.size()));
    return impl_[_i];
  }

  /// Returns the last element
  T &back() {
    assert_true(size() > 0);
    return *(end() - 1);
  }

  /// Returns the last element
  T back() const {
    assert_true(size() > 0);
    return *(end() - 1);
  }

  /// Iterator to the beginning of the vector.
  iterator begin() { return impl_.begin(); }

  /// Iterator to the beginning of the vector.
  const_iterator begin() const { return impl_.begin(); }

  /// The capacity of the vector.
  size_t capacity() const { return impl_.capacity(); }

  /// Returns a pointer to the underlying data.
  T *data() { return impl_.data(); }

  /// Returns a pointer to the underlying data.
  const T *data() const { return impl_.data(); }

  /// Iterator to the beginning of the vector.
  iterator end() { return impl_.end(); }

  /// Iterator to the beginning of the vector.
  const_iterator end() const { return impl_.end(); }

  /// Inserts a new element at the position signified by _pos
  void insert(const size_t _pos, const T _elem) { impl_.insert(_pos, _elem); }

  /// Access operator
  T &operator[](size_t _i) { return impl_[_i]; }

  /// Access operator
  T operator[](size_t _i) const { return impl_[_i]; }

  /// Copy assignment operator.
  Vector<T> &operator=(const Vector<T> &_other) = delete;

  /// Adds a new element on the back of the Vector.
  void push_back(const T _val) { impl_.push_back(_val); }

  /// The size of the vector.
  size_t size() const { return impl_.size(); }

  /// Returns the impl and yields all ownership on the
  /// data. RAII will no longer work and it is now the
  /// programmer's responsibility to deallocate the
  /// resources.
  VectorImpl<T> yield_impl() { return impl_.yield_ressources(); }

 public:
  /// Allocates data on the disk.
  void allocate(const size_t _capacity) { impl_.allocate(_capacity); }

  /// Deallocates the data, if it exists.
  void deallocate() { impl_.deallocate(); }

  /// Trivial (private) accessor
  std::shared_ptr<Pool> pool() {
    assert_true(pool_);
    return pool_;
  }

  /// Trivial (private) accessor
  std::shared_ptr<const Pool> pool() const {
    assert_true(pool_);
    return pool_;
  }

 private:
  /// Implements the move operation.
  void move(Vector<T> *_other);

 private:
  /// Holds the actual data.
  VectorImpl<T> impl_;

  /// The pool containing the actual data.
  std::shared_ptr<Pool> pool_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class T>
void Vector<T>::move(Vector<T> *_other) {
  impl_ = _other->yield_impl();
  pool_ = _other->pool_;
}

// ----------------------------------------------------------------------------

template <class T>
Vector<T> &Vector<T>::operator=(Vector<T> &&_other) noexcept {
  if (this == &_other) {
    return *this;
  }

  deallocate();

  move(&_other);

  return *this;
}

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_VECTOR_HPP_
