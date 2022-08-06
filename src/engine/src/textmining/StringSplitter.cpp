// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "textmining/StringSplitter.hpp"

namespace textmining {
// ----------------------------------------------------------------------------

std::vector<std::string> StringSplitter::split(const std::string& _str) {
  std::vector<std::string> splitted;

  auto remaining = _str;

  while (true) {
    const auto pos = remaining.find_first_of(separators_);

    if (pos == std::string::npos) {
      splitted.push_back(remaining);
      break;
    }

    const auto token = remaining.substr(0, pos);

    splitted.push_back(token);

    remaining.erase(0, pos + 1);
  }

  return splitted;
}

// ----------------------------------------------------------------------------
}  // namespace textmining
