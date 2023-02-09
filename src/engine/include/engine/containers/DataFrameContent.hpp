// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_CONTAINERS_DATAFRAMECONTENT_HPP_
#define ENGINE_CONTAINERS_DATAFRAMECONTENT_HPP_

#include <cstdint>
#include <string>
#include <vector>

#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"

namespace engine {
namespace containers {

/// A format that is compatible with the data.tables API.
using DataFrameContent =
    fct::NamedTuple<fct::Field<"draw", std::int32_t>,
                    fct::Field<"recordsTotal", std::int32_t>,
                    fct::Field<"recordsFiltered", std::int32_t>,
                    fct::Field<"data", std::vector<std::vector<std::string>>>>;

}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_DATAFRAMECONTENT_HPP_
