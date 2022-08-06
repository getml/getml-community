// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "database/QuerySplitter.hpp"

#include "debug/debug.hpp"
#include "fct/fct.hpp"
#include "io/io.hpp"

namespace database {

std::vector<std::string> QuerySplitter::split_queries(
    const std::string& _queries_str) {
  std::string delimiter = ";";

  size_t pos = 0;

  std::vector<std::string> queries;

  while (true) {
    const auto pos_keyword = std::min(_queries_str.find("DELIMITER", pos),
                                      _queries_str.find("delimiter", pos));

    const auto pos_sep = _queries_str.find(delimiter, pos);

    if (pos_keyword != std::string::npos && pos_keyword < pos_sep)
        [[unlikely]] {
      const auto pos_newline = _queries_str.find("\n", pos_keyword);

      if (pos_newline == std::string::npos) {
        break;
      }

      delimiter = io::Parser::trim(
          _queries_str.substr(pos_keyword + 9, pos_newline - pos_keyword - 9));

      pos = pos_newline + 1;

      continue;
    }

    assert_true(pos_sep >= pos);

    const auto query = pos_sep == std::string::npos
                           ? _queries_str.substr(pos)
                           : _queries_str.substr(pos, pos_sep - pos);

    queries.push_back(query);

    if (pos_sep == std::string::npos) {
      break;
    }

    pos = pos_sep + 1;
  }

  return sanitize(queries);
}

// ----------------------------------------------------------------------------

std::vector<std::string> QuerySplitter::sanitize(
    const std::vector<std::string>& _splitted) {
  const auto is_not_empty = [](const std::string& _str) -> bool {
    return _str != "";
  };

  return fct::collect::vector<std::string>(_splitted |
                                           VIEWS::transform(io::Parser::trim) |
                                           VIEWS::filter(is_not_empty));
}

}  // namespace database
