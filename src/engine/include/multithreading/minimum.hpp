// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef MULTITHREADING_MINIMUM_HPP_
#define MULTITHREADING_MINIMUM_HPP_

namespace multithreading {
// ----------------------------------------------------------------------------

template <class T>
struct minimum {
 public:
  minimum() {}

  ~minimum() = default;

  // -----------------------------------------

  T operator()(const T& _elem1, const T& _elem2) {
    return ((_elem1 < _elem2) ? (_elem1) : (_elem2));
  }
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_MINIMUM_HPP_
