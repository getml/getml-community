// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_DATAFRAMECONTENT_HPP_
#define CONTAINERS_DATAFRAMECONTENT_HPP_

#include "database/TableContent.hpp"

namespace containers {

/// A format that is compatible with the data.tables API.
using DataFrameContent = database::TableContent;

}  // namespace containers

#endif  // CONTAINERS_DATAFRAMECONTENT_HPP_
