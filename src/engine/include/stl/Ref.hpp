#ifndef STL_REF_HPP_
#define STL_REF_HPP_

#include <memory>

namespace stl {

/// The Ref class behaves very similarly to the shared_ptr, but unlike the
/// shared_ptr, it is 100% guaranteed to be filled at all times.
/// This saves us from a lot of checks.
template <class T>
class Ref {
 public:
  template <class... Args>
  explicit Ref(Args... _args) : ptr_(std::make_shared<T>(_args...)) {}

  template <class Y>
  Ref(const Ref<Y>& _other) : ptr_(_other.ptr_) {}

  ~Ref() = default;

  /// Returns a pointer to the underlying object
  inline T* get() const { return ptr_.get(); }

  /// Returns the underlying object.
  inline T& operator*() const { return *ptr_; }

  /// Returns the underlying object.
  inline T& operator->() const { return *ptr_; }

  /// Copy assignment operator.
  template <class Y>
  Ref<T>& operator=(const Ref<Y>& _other) {
    ptr_ = _other.ptr_;
  }

 private:
  /// The underlying shared_ptr_
  std::shared_ptr<T> ptr_;
};

}  // namespace stl

#endif  // STL_REF_HPP_

