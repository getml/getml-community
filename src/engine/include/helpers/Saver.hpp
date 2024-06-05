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

#include <rfl/Literal.hpp>
#include <rfl/always_false.hpp>
#include <rfl/json.hpp>
#include <rfl/visit.hpp>

namespace helpers {

struct Saver {
  using Format = rfl::Literal<"json">;

  /// Saves the object in one of the supported formats.
  template <class T>
  static void save(const std::string& _fname, const T& _obj,
                   const Format& _format) {
    const auto handle_variant = [&]<typename U>(const U& _format) {
      if constexpr (std::is_same<U, rfl::Literal<"json">>()) {
        save_as_json(_fname, _obj);
      } else {
        static_assert(rfl::always_false_v<U>, "Not all cases were supported");
      }
    };
    rfl::visit(handle_variant, _format);
  }

  /// Saves any class that is supported by the json library to
  /// JSON.
  template <class T>
  static void save_as_json(const std::string& _fname, const T& _obj) {
    const auto json_str = rfl::json::write(_obj);
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
