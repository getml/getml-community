// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef STRINGS_STRINGHASHER_HPP_
#define STRINGS_STRINGHASHER_HPP_

// ----------------------------------------------------------------------------

#include "strings/String.hpp"

// ----------------------------------------------------------------------------

namespace strings {

// ----------------------------------------------------------------------------

struct StringHasher {
  std::size_t operator()(const String& _str) const { return _str.hash(); }
};

// ----------------------------------------------------------------------------
}  // namespace strings

#endif  // STRINGS_STRINGHASHER_HPP_
