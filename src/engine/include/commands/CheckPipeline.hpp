// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_CHECK_PIPELINE_HPP_
#define COMMANDS_CHECK_PIPELINE_HPP_

#include <optional>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/define_named_tuple.hpp"

namespace commands {

using CheckPipeline = typename rfl::define_named_tuple<
    rfl::Field<"type_", rfl::Literal<"Pipeline.check">>,
    rfl::Field<"name_", std::string>, DataFramesOrViews>::type;

}  // namespace commands

#endif  // COMMANDS_CHECK_PIPELINE_HPP_
