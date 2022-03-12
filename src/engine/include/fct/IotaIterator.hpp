#ifndef FCT_IOTAITERATOR_HPP_
#define FCT_IOTAITERATOR_HPP_

// -------------------------------------------------------------------------

#include <iterator>

// -------------------------------------------------------------------------

namespace fct {

template <class T>
class IotaIterator {
 public:
  using iterator_category = std::bidirectional_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = T;
  using pointer = value_type*;
  using reference = value_type&;

  /// Default constructor.
  IotaIterator() : i_(0), ptr_(&i_) {}

  /// Construct by value.
  explicit IotaIterator(const value_type& _i) : i_(_i), ptr_(&i_) {}

  /// Copy constructor.
  IotaIterator(const IotaIterator<T>& _other) : i_(_other.i_), ptr_(&i_) {}

  /// Move constructor.
  IotaIterator(IotaIterator<T>&& _other) noexcept : i_(_other.i_), ptr_(&i_) {}

  /// Destructor.
  ~IotaIterator() = default;

  /// Copy assignment operator.
  IotaIterator<T>& operator=(const IotaIterator<T>& _other) {
    i_ = _other.i_;
    return *this;
  }

  /// Move assignment operator.
  IotaIterator<T>& operator=(IotaIterator<T>&& _other) noexcept {
    i_ = _other.i_;
    return *this;
  }

  /// Tricky: operator()* must be const, but provide a non-const reference.
  inline reference operator*() const { return *ptr_; }

  /// Returns a pointer to value_.
  inline pointer operator->() { return ptr_; }

  /// Prefix incrementor
  inline IotaIterator<T>& operator++() {
    ++i_;
    return *this;
  }

  /// Postfix incrementor.
  inline IotaIterator<T> operator++(int) {
    IotaIterator<T> tmp = *this;
    ++(*this);
    return tmp;
  }

  /// Prefix decrementor
  inline IotaIterator<T>& operator--() {
    --i_;
    return *this;
  }

  /// Postfix decrementor.
  inline IotaIterator<T> operator--(int) {
    IotaIterator<T> tmp = *this;
    --(*this);
    return tmp;
  }

  /// Check equality.
  friend inline bool operator==(const IotaIterator<T>& _a,
                                const IotaIterator<T>& _b) {
    return _a.i_ == _b.i_;
  }

  /// Check inequality.
  friend inline bool operator!=(const IotaIterator<T>& _a,
                                const IotaIterator<T>& _b) {
    return _a.i_ != _b.i_;
  }

 private:
  /// The current value.
  value_type i_;

  /// A pointer to the current value - necessary workaround.
  pointer ptr_;
};

// -------------------------------------------------------------------------
}  // namespace fct

#endif  // FCT_IOTAITERATOR_HPP_
