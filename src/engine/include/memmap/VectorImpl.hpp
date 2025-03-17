// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef MEMMAP_VECTORIMPL_HPP_
#define MEMMAP_VECTORIMPL_HPP_

#include "debug/assert_msg.hpp"
#include "memmap/Pool.hpp"

#include <cstddef>

namespace memmap {
// ----------------------------------------------------------------------------
// RAII does not work in a memory mapped environment.
// We have to allocate and deallocate manually.
// VectorImpl is meant to be allocated inside a memory
// map, but it also underlies the RAII-friendly Vector
// class.

template <class T>
class VectorImpl {
 public:
  constexpr static size_t NOT_ALLOCATED = Pool::NOT_ALLOCATED;

  using iterator = T *;
  using const_iterator = const T *;

 public:
  VectorImpl() : page_num_(NOT_ALLOCATED), pool_(nullptr), size_(0) {}

  VectorImpl(const size_t _page_num, Pool *_pool, const size_t _size)
      : page_num_(_page_num), pool_(_pool), size_(_size) {
    assert_true(pool_);
    assert_true(!is_allocated() || page_num_ < pool_->num_pages());
    assert_true(!is_allocated() || size_ < capacity());
  }

  ~VectorImpl() = default;

 public:
  /// Inserts a new element at the position signified by _pos
  void insert(const size_t _pos, const T _elem);

  /// Adds a new element on the back of the VectorImpl.
  void push_back(const T _val);

 public:
  /// Allocates data on the disk.
  void allocate(const size_t _capacity) {
    assert_true(pool_);
    page_num_ = pool_->allocate<T>(_capacity, page_num_);
  }

  /// Iterator to the beginning of the vector.
  iterator begin() { return data(); }

  /// Iterator to the beginning of the vector.
  const_iterator begin() const { return data(); }

  /// Returns the capacity.
  size_t capacity() const {
    assert_true(pool_);
    assert_true(is_allocated());
    return pool_->capacity<T>(page_num_);
  }

  /// Returns a pointer to the underlying data.
  T *data() {
    assert_true(pool_);
    return pool_->addr<T>(page_num_);
  }

  /// Returns a pointer to the underlying data.
  const T *data() const {
    assert_true(pool_);
    return pool_->addr<T>(page_num_);
  }

  /// Deallocates the data, if it exists.
  void deallocate() {
    if (is_allocated()) {
      assert_true(pool_);
      pool_->deallocate(page_num_);
      page_num_ = NOT_ALLOCATED;
      size_ = 0;
    }
  }

  /// Iterator to the beginning of the vector.
  iterator end() { return data() + size_; }

  /// Iterator to the beginning of the vector.
  const_iterator end() const { return data() + size_; }

  /// Whether the VectorImpl is allocated
  bool is_allocated() const {
    assert_true(page_num_ == NOT_ALLOCATED || pool_);
    assert_true(page_num_ != NOT_ALLOCATED || size_ == 0);
    return (page_num_ != NOT_ALLOCATED);
  }

  /// Access operator
  T &operator[](size_t _i) {
    assert_msg(_i < size_, "_i: " + std::to_string(_i) +
                               ", size_: " + std::to_string(size_));
    return *(data() + _i);
  }

  /// Access operator
  T operator[](size_t _i) const {
    assert_msg(_i < size_, "_i: " + std::to_string(_i) +
                               ", size_: " + std::to_string(size_));
    return *(data() + _i);
  }

  /// Trivial (const) accessor
  size_t page_num() const { return page_num_; }

  /// Trivial (const) accessor
  Pool *pool() const { return pool_; }

  /// Trivial (const) accessor
  size_t size() const { return size_; }

  /// Yields all resources - when we call deallocate on
  /// this VectorImpl after that, nothing will happen.
  VectorImpl<T> yield_ressources() {
    const auto impl = *this;
    *this = VectorImpl<T>();
    return impl;
  };

 private:
  /// Signifies the first page of the memory block.
  size_t page_num_;

  /// Points to the underlying pool.
  Pool *pool_;

  /// The number of filled elements in the vector.
  size_t size_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class T>
void VectorImpl<T>::insert(const size_t _pos, const T _elem) {
  assert_true(_pos <= size());

  if (size_ == capacity()) {
    const auto new_capacity = size_ * 2;
    allocate(new_capacity);
  }

  auto ptr = data();

  for (auto i = size(); i > _pos; --i) {
    *(ptr + i) = *(ptr + i - 1);
  }

  *(ptr + _pos) = _elem;

  ++size_;
}

// ----------------------------------------------------------------------------

template <class T>
void VectorImpl<T>::push_back(const T _val) {
  assert_true(is_allocated());

  if (size_ == capacity()) {
    const auto new_capacity = size_ * 2;
    allocate(new_capacity);
  }

  data()[size_++] = _val;
}

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_VECTORIMPL_HPP_
