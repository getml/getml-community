// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/StringSplitter.hpp"

namespace helpers {
// ----------------------------------------------------------------------------

std::vector<std::string> StringSplitter::split(const std::string& _str,
                                               const std::string& _sep) {
  std::vector<std::string> splitted;

  size_t begin = 0;

  while (true) {
    const auto pos = _str.find(_sep, begin);

    const auto len = pos == std::string::npos ? std::string::npos : pos - begin;

    const auto token = _str.substr(begin, len);

    splitted.push_back(token);

    if (pos == std::string::npos) {
      break;
    }

    begin += _sep.size() + token.size();
  }

  return splitted;
}

// ----------------------------------------------------------------------------
}  // namespace helpers
