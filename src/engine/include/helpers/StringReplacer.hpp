// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef HELPERS_STRINGREPLACER_HPP_
#define HELPERS_STRINGREPLACER_HPP_

#include <string>

namespace helpers {
// ----------------------------------------------------------------------------

struct StringReplacer {
  /// Replaces all instances of _from in _str with _to.
  static std::string replace_all(const std::string &_str,
                                 const std::string &_from,
                                 const std::string &_to);
};

// ----------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_STRINGREPLACER_HPP_

