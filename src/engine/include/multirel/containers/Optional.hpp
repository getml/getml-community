#ifndef MULTIREL_CONTAINERS_OPTIONAL_HPP_
#define MULTIREL_CONTAINERS_OPTIONAL_HPP_

// -------------------------------------------------------------------------

#include <memory>
#include <utility>

// -------------------------------------------------------------------------

#include "debug/debug.hpp"

// -------------------------------------------------------------------------

namespace multirel {
namespace containers {
// -------------------------------------------------------------------------
// The purpose of the Optional class is to be a wrapper around
// std::unique_ptr that contains a copy operator for creating deep copies.

// We are applying the "Rule of Five" - the only exception is that we
// are using the default destructor, because this is based on an unique_ptr.

template <class T>
class Optional {
 public:
  Optional() {}

  Optional(T* _ptr) : thisptr_(_ptr) {}

  Optional(const Optional& _other) {
    if (_other.thisptr_) {
      thisptr().reset(new T(*_other.thisptr().get()));
    }
  }

  Optional(Optional&& _other) noexcept : thisptr_(std::move(_other.thisptr_)) {}

  ~Optional() = default;

  // -------------------------------

  inline T* get() {
    assert_true(thisptr() && "Optional.get() called on empty Optional!");

    return thisptr().get();
  }

  inline const T* get() const {
    assert_true(thisptr() && "Optional.get() called on empty Optional!");

    return thisptr().get();
  }

  operator bool() const { return (thisptr() ? true : false); }

  Optional& operator=(const Optional& _other) {
    Optional temp(_other);
    *this = std::move(temp);
    return *this;
  }

  Optional& operator=(Optional&& _other) noexcept {
    if (this == &_other) {
      return *this;
    }

    thisptr() = std::move(_other.thisptr_);

    return *this;
  }

  T& operator*() {
    assert_true(thisptr() && "Optional* called on empty Optional!");

    return *thisptr().get();
  }

  const T& operator*() const {
    assert_true(thisptr() && "Optional* called on empty Optional!");

    return *thisptr().get();
  }

  T* operator->() {
    assert_true(thisptr() && "Optional-> called on empty Optional!");

    return thisptr().get();
  }

  const T* operator->() const {
    assert_true(thisptr() && "Optional-> called on empty Optional!");

    return thisptr().get();
  }

  inline void reset() { thisptr().reset(); }

  inline void reset(T* _ptr) { thisptr().reset(_ptr); }

  // -------------------------------

 private:
  /// Trivial accessor
  std::unique_ptr<T>& thisptr() { return thisptr_; }

  /// Trivial accessor
  const std::unique_ptr<T>& thisptr() const { return thisptr_; }

  // -------------------------------

 private:
  // std::unique_ptr to which Optional
  // is a wrapper.
  std::unique_ptr<T> thisptr_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace multirel

#endif  // MULTIREL_CONTAINERS_OPTIONAL_HPP_
