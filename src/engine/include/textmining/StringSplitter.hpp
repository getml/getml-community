// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef TEXTMINING_STRINGSPLITTER_HPP_
#define TEXTMINING_STRINGSPLITTER_HPP_

// ----------------------------------------------------------------------------

#include <string>
#include <vector>

// ----------------------------------------------------------------------------

namespace textmining {
// ----------------------------------------------------------------------------

/// The StringSplitter in textmining differs from the StringSplitter in helpers
/// in that it can separate on more than just one character.
struct StringSplitter {
  static constexpr const char* separators_ = " ;,.!?-|\t\"\t\v\f\r\n%'()[]{}";

  /// Splits a string into its individual components.
  static std::vector<std::string> split(const std::string& _str);
};

// ----------------------------------------------------------------------------
}  // namespace textmining

#endif  // TEXTMINING_STRINGSPLITTER_HPP_
