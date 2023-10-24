// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/preprocessors/PreprocessorImpl.hpp"

namespace engine {
namespace preprocessors {

std::vector<std::string> PreprocessorImpl::retrieve_names(
    const MarkerType _marker, const size_t _table,
    const std::vector<fct::Ref<helpers::ColumnDescription>>& _desc) {
  const auto table = std::to_string(_table);

  auto names = std::vector<std::string>();

  for (const auto& ptr : _desc) {
    if (ptr->marker() == _marker && ptr->table() == table) {
      names.push_back(ptr->name());
    }
  }

  return names;
}

}  // namespace preprocessors
}  // namespace engine
