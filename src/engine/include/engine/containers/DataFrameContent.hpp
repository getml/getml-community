// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_CONTAINERS_DATAFRAMECONTENT_HPP_
#define ENGINE_CONTAINERS_DATAFRAMECONTENT_HPP_

#include "database/TableContent.hpp"

namespace engine {
namespace containers {

/// A format that is compatible with the data.tables API.
using DataFrameContent = database::TableContent;

}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_DATAFRAMECONTENT_HPP_
