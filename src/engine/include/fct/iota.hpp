// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef FCT_IOTA_HPP_
#define FCT_IOTA_HPP_

#include "fct/ranges.hpp"

namespace fct {

/// Necessary work-around, as iota is not supported on Windows yet.
template <class T, class T1, class T2>
inline auto iota(T1 _begin, T2 _end) {
#if (defined(_WIN32) || defined(_WIN64))
  return IotaRange<T>(static_cast<T>(_begin), static_cast<T>(_end));
#else
  return VIEWS::iota(static_cast<T>(_begin), static_cast<T>(_end));
#endif
}

}  // namespace fct

#endif  // FCT_IOTA_HPP_
