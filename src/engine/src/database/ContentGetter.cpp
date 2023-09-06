// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "database/ContentGetter.hpp"

#include <string>
#include <vector>

namespace database {

TableContent ContentGetter::get_content(const rfl::Ref<Iterator>& _iter,
                                        const std::int32_t _draw,
                                        const std::int32_t _records_filtered,
                                        const std::int32_t _records_total,
                                        const std::int32_t _ncols) {
  const auto basis = rfl::make_field<"draw">(_draw) *
                     rfl::make_field<"recordsTotal">(_records_total) *
                     rfl::make_field<"recordsFiltered">(_records_filtered);

  if (_records_total == 0) {
    return basis *
           rfl::make_field<"data">(std::vector<std::vector<std::string>>());
  }

  auto data = std::vector<std::vector<std::string>>(_records_filtered);

  for (std::int32_t i = 0; i < _records_filtered; ++i) {
    auto row = std::vector<std::string>(_ncols);

    for (std::int32_t j = 0; j < _ncols; ++j) {
      row.at(j) = _iter->get_string();
    }

    data.at(i) = row;
  }

  return basis * rfl::make_field<"data">(data);
}

}  // namespace database
