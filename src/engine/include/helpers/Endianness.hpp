// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef HELPERS_ENDIANNESS_HPP_
#define HELPERS_ENDIANNESS_HPP_

#include <algorithm>

namespace helpers {

struct Endianness {
  /// Determines endianness of the system at runtime
  static bool is_little_endian() {
    unsigned int one = 1;
    char *c = reinterpret_cast<char *>(&one);
    return *c;
  }

  /// Reverses the byteorder of the value passed to it
  template <typename T>
  static void reverse_byte_order(T *_val) {
    char *v = reinterpret_cast<char *>(_val);
    std::reverse(v, v + sizeof(T));
  }
};

}  // namespace helpers

#endif  // HELPERS_ENDIANNESS_HPP_
