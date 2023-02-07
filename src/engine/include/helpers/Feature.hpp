// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_FEATURE_HPP_
#define HELPERS_FEATURE_HPP_

#include <memory>
#include <optional>

#include "debug/debug.hpp"
#include "helpers/Column.hpp"
#include "memmap/Pool.hpp"

namespace helpers {

/// Unlike a Column<T>, a Feature is mutable and can only hold
/// floating point values (therefore the complexity related to memory-mapped
/// string columns does not apply here).
template <class T, bool safe_mode_ = true>
class Feature {
 public:
  typedef typename Column<T>::MemmapVector MemmapVector;
  typedef typename Column<T>::InMemoryVector InMemoryVector;

  typedef typename Column<T>::InMemoryPtr InMemoryPtr;
  typedef typename Column<T>::MemmapPtr MemmapPtr;

  typedef typename Column<T>::ConstInMemoryPtr ConstInMemoryPtr;
  typedef typename Column<T>::ConstMemmapPtr ConstMemmapPtr;

  typedef typename Column<T>::Variant Variant;
  typedef typename Column<T>::ConstVariant ConstVariant;

 public:
  Feature(const Variant& _ptr = InMemoryPtr())
      : data_(safe_mode_ ? nullptr : get_data(_ptr)),
        ptr_(_ptr),
        size_(safe_mode_ ? 0 : get_size(_ptr)) {
    static_assert(
        std::is_floating_point<T>::value || std::is_integral<T>::value,
        "T must floating point or integral!");
  }

  Feature(const std::shared_ptr<memmap::Pool>& _pool = nullptr,
          const size_t _size)
      : Feature(_pool
                    ? Feature(std::make_shared<memmap::Vector<T>>(_pool, _size))
                    : Feature(std::make_shared<std::vector<T>>(_size))) {}

  ~Feature() = default;

 public:
  /// Access operator
  T& at(const size_t _i) {
    throw_unless(begin(), "Feature: No data available.");
    throw_unless(_i < size(), "Access out of range. _i: " + std::to_string(_i) +
                                  ", size: " + std::to_string(size()));
    return *(begin() + _i);
  }

  /// Access operator
  T at(const size_t _i) const {
    throw_unless(begin(), "Feature: No data available.");
    throw_unless(_i < size(), "Access out of range. _i: " + std::to_string(_i) +
                                  ", size: " + std::to_string(size()));
    return *(begin() + _i);
  }

  /// Iterator begin
  T* begin() {
    if constexpr (safe_mode_) {
      return get_data(ptr_);
    } else {
      assert_true(pointer_is_aligned());
      return data_;
    }
  }

  /// Iterator begin
  const T* begin() const {
    if constexpr (safe_mode_) {
      return get_data(ptr_);
    } else {
      assert_true(pointer_is_aligned());
      return data_;
    }
  }

  /// Trivial (const) accessor.
  ConstVariant const_ptr() const {
    if (std::holds_alternative<InMemoryPtr>(ptr_)) {
      return std::get<InMemoryPtr>(ptr_);
    }
    assert_true(std::holds_alternative<MemmapPtr>(ptr_));
    return std::get<MemmapPtr>(ptr_);
  }

  /// Pointer to the data
  T* data() { return begin(); }

  /// Iterator begin
  const T* data() const { return begin(); }

  /// Iterator end
  T* end() { return begin() + size(); }

  /// Iterator end
  const T* end() const { return begin() + size(); }

  /// Whether the feature is memory mapped.
  bool is_memory_mapped() const {
    return std::holds_alternative<MemmapPtr>(ptr_);
  }

  /// Access operator
  T& operator[](const size_t _i) {
    assert_true(begin());
    assert_msg(_i < size(), "_i: " + std::to_string(_i) +
                                ", size(): " + std::to_string(size()));
    return *(begin() + _i);
  }

  /// Access operator
  T operator[](const size_t _i) const {
    assert_true(begin());
    assert_msg(_i < size(), "_i: " + std::to_string(_i) +
                                ", size(): " + std::to_string(size()));
    return *(begin() + _i);
  }

  /// Trivial accessor.
  Variant ptr() const { return ptr_; }

  /// Trivial (const) accessor.
  size_t size() const {
    if constexpr (safe_mode_) {
      return get_size(ptr_);
    } else {
      return size_;
    }
  }

  /// Retrieve the underlying pool, if the column is memory mapped.
  std::shared_ptr<memmap::Pool> pool() const {
    if (std::holds_alternative<InMemoryPtr>(ptr_)) {
      return nullptr;
    }
    assert_true(std::holds_alternative<MemmapPtr>(ptr_));
    assert_true(std::get<MemmapPtr>(ptr_)->pool());
    return std::get<MemmapPtr>(ptr_)->pool();
  }

 private:
  /// In the memory mapped version, it is possible that the
  /// the pointer becomes misaligned, if the pool has been
  /// accidentally reused. So we make sure that this doesn't
  /// happen in DEBUG mode.
#ifndef NDEBUG
  template <bool _unsafe_mode = !safe_mode_,
            typename std::enable_if<_unsafe_mode, int>::type = 0>
  bool pointer_is_aligned() const {
    return (data_ == get_data(ptr_));
  }
#endif  // NDEBUG

  /// Retrieves the data.
  static T* get_data(const Variant _ptr) {
    if (std::holds_alternative<InMemoryPtr>(_ptr) &&
        !std::get<InMemoryPtr>(_ptr)) {
      return nullptr;
    }

    if (std::holds_alternative<MemmapPtr>(_ptr) && !std::get<MemmapPtr>(_ptr)) {
      return nullptr;
    }

    return std::holds_alternative<InMemoryPtr>(_ptr)
               ? std::get<InMemoryPtr>(_ptr)->data()
               : std::get<MemmapPtr>(_ptr)->data();
  }

  /// Retrieves the number of rows.
  static size_t get_size(const Variant _ptr) {
    if (std::holds_alternative<InMemoryPtr>(_ptr) &&
        !std::get<InMemoryPtr>(_ptr)) {
      return 0;
    }

    if (std::holds_alternative<MemmapPtr>(_ptr) && !std::get<MemmapPtr>(_ptr)) {
      return 0;
    }

    return std::holds_alternative<InMemoryPtr>(_ptr)
               ? std::get<InMemoryPtr>(_ptr)->size()
               : std::get<MemmapPtr>(_ptr)->size();
  }

 private:
  /// Pointer to the underlying data, for optimized access.
  T* data_;

  /// The pointer to take ownership of the underlying data.
  Variant ptr_;

  /// Number of rows
  size_t size_;
};

}  // namespace helpers

#endif  // HELPERS_COLUMN_HPP_
