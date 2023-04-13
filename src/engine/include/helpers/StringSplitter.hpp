// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_STRINGSPLITTER_HPP_
#define HELPERS_STRINGSPLITTER_HPP_

#include <string>
#include <vector>

namespace helpers {

struct StringSplitter {
  /// Splits a string into its individual components.
  static std::vector<std::string> split(const std::string& _str,
                                        const std::string& _sep);
};

}  // namespace helpers

#endif  // HELPERS_STRINGSPLITTER_HPP_
