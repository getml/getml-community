// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_VIEWCONTENT_HPP_
#define CONTAINERS_VIEWCONTENT_HPP_

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "containers/DataFrameContent.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"

namespace containers {

/// A format that is compatible with the data.tables API.
using ViewContent = std::variant<
    DataFrameContent,
    fct::NamedTuple<fct::Field<"draw", std::int32_t>,
                    fct::Field<"data", std::vector<std::vector<std::string>>>>>;

}  // namespace containers

#endif  // CONTAINERS_VIEWCONTENT_HPP_
