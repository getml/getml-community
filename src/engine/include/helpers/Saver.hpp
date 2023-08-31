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

#include "fct/Literal.hpp"
#include "fct/always_false.hpp"
#include "fct/visit.hpp"
#include "flexbuffers/to_flexbuffers.hpp"
#include "json/to_json.hpp"

namespace helpers {

struct Saver {
  using Format = fct::Literal<"flexbuffers", "json">;

  /// Saves the object in one of the supported formats.
  template <class T>
  static void save(const std::string& _fname, const T& _obj,
                   const Format& _format) {
    const auto handle_variant = [&]<typename U>(const U& _format) {
      if constexpr (std::is_same<U, fct::Literal<"flexbuffers">>()) {
        save_as_flexbuffers(_fname, _obj);
      } else if constexpr (std::is_same<U, fct::Literal<"json">>()) {
        save_as_json(_fname, _obj);
      } else {
        static_assert(fct::always_false_v<U>, "Not all cases were supported");
      }
    };
    fct::visit(handle_variant, _format);
  }

  /// Saves any class that is supported by the flexbuffers library to
  /// a flexbuffers file.
  template <class T>
  static void save_as_flexbuffers(const std::string& _fname, const T& _obj) {
    const auto bytes = flexbuffers::to_flexbuffers(_obj);
    const auto fname =
        _fname.size() > 3 && _fname.substr(_fname.size() - 3) == ".fb"
            ? _fname
            : _fname + ".fb";
    std::ofstream output(fname, std::ios::out | std::ios::binary);
    output.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    output.close();
  }

  /// Saves any class that is supported by the json library to
  /// JSON.
  template <class T>
  static void save_as_json(const std::string& _fname, const T& _obj) {
    const auto json_str = json::to_json(_obj);
    const auto fname =
        _fname.size() > 5 && _fname.substr(_fname.size() - 5) == ".json"
            ? _fname
            : _fname + ".json";
    std::ofstream output(fname);
    output << json_str;
    output.close();
  }
};

}  // namespace helpers

#endif  // HELPERS_SAVER_HPP_
