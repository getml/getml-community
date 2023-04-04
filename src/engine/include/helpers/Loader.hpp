// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_LOADER_HPP_
#define HELPERS_LOADER_HPP_

#include <fstream>
#include <sstream>
#include <string>

#include "json/json.hpp"

namespace helpers {

class Loader {
 public:
  /// Saves any class that is supported by the json library to
  /// JSON.
  template <class T>
  static T load_from_json(const std::string& _fname) {
    const auto fname =
        _fname.size() > 5 && _fname.substr(_fname.size() - 5) == ".json"
            ? _fname
            : _fname + ".json";
    const auto json_str = read_str(fname);
    return json::from_json<T>(json_str);
  }

 private:
  /// Reads a string from a file.
  static std::string read_str(const std::string& _fname) {
    std::ifstream input(_fname);

    std::stringstream stream;

    std::string line;

    if (input.is_open()) {
      while (std::getline(input, line)) {
        stream << line;
      }

      input.close();
    } else {
      throw std::runtime_error("File '" + _fname + "' not found!");
    }

    return stream.str();
  }
};

}  // namespace helpers

#endif  // HELPERS_LOADER_HPP_
