// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_GETCONTENT_HPP_
#define COMMANDS_GETCONTENT_HPP_

#include <cstddef>

#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"

namespace commands {

/// Defines the three fields necessary to the content of a column.
using GetContent =
    fct::NamedTuple<fct::Field<"draw_", Int>, fct::Field<"length_", size_t>,
                    fct::Field<"start_", size_t>>;

}  // namespace commands

#endif  // COMMANDS_GETCONTENT_HPP_
