// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_VIEWCONTENT_HPP_
#define CONTAINERS_VIEWCONTENT_HPP_

#include "containers/DataFrameContent.hpp"

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace containers {

/// A format that is compatible with the data.tables API.
using ViewContent = std::variant<
    DataFrameContent,
    rfl::NamedTuple<rfl::Field<"draw", std::int32_t>,
                    rfl::Field<"data", std::vector<std::vector<std::string>>>>>;

}  // namespace containers

#endif  // CONTAINERS_VIEWCONTENT_HPP_
