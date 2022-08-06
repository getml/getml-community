// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

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
