// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_SAVER_HPP_
#define HELPERS_SAVER_HPP_

#include <fstream>
#include <string>

#include "flexbuffers/to_flexbuffers.hpp"
#include "json/to_json.hpp"

namespace helpers {

struct Saver {
  /// Saves any class that is supported by the flexbuffers library to
  /// a flexbuffers file.
  template <class T>
  static void save_as_flexbuffers(const std::string& _fname, const T& _obj) {
    const auto bytes = flexbuffers::to_flexbuffers(_obj);
    std::ofstream output(_fname);
    for (const auto b : bytes) {
      output << b;
    }
    output.close();
  }

  /// Saves any class that is supported by the json library to
  /// JSON.
  template <class T>
  static void save_as_json(const std::string& _fname, const T& _obj) {
    const auto json_str = json::to_json(_obj);
    std::ofstream output(_fname);
    output << json_str;
    output.close();
  }
};

}  // namespace helpers

#endif  // HELPERS_SAVER_HPP_
