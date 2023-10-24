// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "textmining/Vocabulary.hpp"

#include "textmining/StringSplitter.hpp"

namespace textmining {
// ----------------------------------------------------------------------------

std::set<std::string> Vocabulary::process_text_field(
    const strings::String& _text_field) {
  const auto make_unique =
      [](const std::vector<std::string>& _tokens) -> std::set<std::string> {
    return std::set<std::string>(_tokens.begin(), _tokens.end());
  };

  return make_unique(split_text_field(_text_field));
}

// ----------------------------------------------------------------------------

std::vector<std::string> Vocabulary::split_text_field(
    const strings::String& _text_field) {
  const auto is_non_empty = [](const std::string& str) -> bool {
    return str.size() > 0;
  };

  const auto remove_empty = [is_non_empty](const auto& vec) {
    return fct::collect::vector(vec | VIEWS::filter(is_non_empty));
  };

  const auto splitted = StringSplitter::split(_text_field.to_lower().str());

  return remove_empty(splitted);
}

// ----------------------------------------------------------------------------
}  // namespace textmining
