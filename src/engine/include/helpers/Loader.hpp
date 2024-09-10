// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_LOADER_HPP_
#define HELPERS_LOADER_HPP_

#include <filesystem>
#include <fstream>
#include <functional>
#include <rfl/json/read.hpp>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace helpers {

class Loader {
 public:
  /// Loads a file, automatically inferrint the ending.
  template <class T>
  static T load(const std::string& _fname) {
    const auto endings = std::vector<
        std::pair<std::string, std::function<T(const std::string&)>>>(
        {std::make_pair(".json", load_from_json<T>)});

    for (const auto& [e, f] : endings) {
      if (_fname.size() > e.size() &&
          _fname.substr(_fname.size() - e.size()) == e) {
        return f(_fname);
      }
    }

    for (const auto& [e, f] : endings) {
      if (std::filesystem::exists(_fname + e)) {
        return f(_fname + e);
      }
    }

    throw std::runtime_error("File '" + _fname + "' not found!");
  }

  /// Loads any class that is supported by the json library from
  /// JSON.
  template <class T>
  static T load_from_json(const std::string& _fname) {
    const auto json_str = read_str(_fname);
    return rfl::json::read<T>(json_str).value();
  }

 private:
  /// Reads bytes from a file.
  static std::vector<unsigned char> read_bytes(const std::string& _fname) {
    std::ifstream input(_fname, std::ios::binary);
    if (input.is_open()) {
      std::istreambuf_iterator<char> begin(input), end;
      const auto bytes = std::vector<unsigned char>(begin, end);
      input.close();
      return bytes;
    } else {
      throw std::runtime_error("File '" + _fname + "' not found!");
    }
  }

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
