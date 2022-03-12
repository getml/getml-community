#ifndef FCT_IOTARANGE_HPP_
#define FCT_IOTARANGE_HPP_

#include "fct/IotaIterator.hpp"

namespace fct {
// -------------------------------------------------------------------------

template <class T>
class IotaRange {
 public:
  IotaRange(T _begin, T _end)
      : begin_(IotaIterator<T>(_begin)), end_(IotaIterator<T>(_end)) {}

  ~IotaRange() = default;

  /// Returns iterator to beginning.
  IotaIterator<T> begin() const { return begin_; }

  /// Returns iterator to end.
  IotaIterator<T> end() const { return end_; }

 private:
  /// Iterator to beginning.
  IotaIterator<T> begin_;

  /// Iterator to end.
  IotaIterator<T> end_;
};

// -------------------------------------------------------------------------
}  // namespace fct

#endif  // FCT_IOTARANGE_HPP_
