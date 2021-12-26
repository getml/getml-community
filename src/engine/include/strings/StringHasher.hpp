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
