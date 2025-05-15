// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "database/DatabaseReader.hpp"

namespace database {

DatabaseReader::DatabaseReader(const rfl::Ref<Iterator>& _iterator)
    : iterator_(_iterator), ncols_(iterator()->colnames().size()) {}

}  // namespace database
