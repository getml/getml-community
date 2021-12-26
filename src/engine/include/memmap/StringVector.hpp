#ifndef MEMMAP_STRINGVECTOR_HPP_
#define MEMMAP_STRINGVECTOR_HPP_

// ----------------------------------------------------------------------------

#include <cstddef>

// ----------------------------------------------------------------------------

#include <memory>

// ----------------------------------------------------------------------------

#include "strings/strings.hpp"

// ----------------------------------------------------------------------------

#include "memmap/Pool.hpp"
#include "memmap/Vector.hpp"

// ----------------------------------------------------------------------------

namespace memmap {
// ----------------------------------------------------------------------------

class StringVector {
 public:
  explicit StringVector(const std::shared_ptr<Pool> &_pool)
      : data_(Vector<char>(_pool)), indptr_(Vector<size_t>(_pool)) {
    indptr_.push_back(0);
  }

  template <class IteratorType>
  StringVector(const std::shared_ptr<Pool> &_pool, IteratorType _begin,
               IteratorType _end)
      : StringVector(_pool) {
    for (auto it = _begin; it != _end; ++it) {
      push_back(*it);
    }
  }

  StringVector(const StringVector &_other) = delete;

  ~StringVector() = default;

 public:
  /// Access operator with bound checks
  strings::String at(size_t _i) const {
    throw_unless(_i < size(), "Out of bounds. i: " + std::to_string(_i) +
                                  ", size: " + std::to_string(size()));
    return strings::String(data_.begin() + indptr_[_i],
                           indptr_[_i + 1] - indptr_[_i]);
  }

  /// Access operator
  strings::String operator[](size_t _i) const {
    return strings::String(data_.begin() + indptr_[_i],
                           indptr_[_i + 1] - indptr_[_i]);
  }

  /// Copy assignment operator.
  StringVector &operator=(const StringVector &_other) = delete;

  /// Adds a new element on the back of the Vector.
  void push_back(const strings::String &_str) {
    const auto len = _str.size();

    for (size_t i = 0; i < len; ++i) {
      data_.push_back(_str.c_str()[i]);
    }

    indptr_.push_back(indptr_.back() + len);
  }

  /// The size of the vector.
  size_t size() const { return indptr_.size() - 1; }

 private:
  /// Holds the actual data.
  Vector<char> data_;

  /// A vector indicating the beginning and end of every single string.
  Vector<size_t> indptr_;
};

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_STRINGVECTOR_HPP_
