// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DATABASE_CONTENTGETTER_HPP_
#define DATABASE_CONTENTGETTER_HPP_

#include <cstdint>
#include <rfl/Ref.hpp>

#include "database/Iterator.hpp"
#include "database/TableContent.hpp"

namespace database {

struct ContentGetter {
  /// Returns the content of a table using the iterator.
  static TableContent get_content(const rfl::Ref<Iterator>& _iter,
                                  const std::int32_t _draw,
                                  const std::int32_t _records_filtered,
                                  const std::int32_t _records_total,
                                  const std::int32_t _ncols);
};

}  // namespace database

#endif  // DATABASE_CONTENTGETTER_HPP_
