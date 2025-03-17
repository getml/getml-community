// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_CHECK_PIPELINE_HPP_
#define COMMANDS_CHECK_PIPELINE_HPP_

#include "commands/DataFramesOrViews.hpp"

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/define_named_tuple.hpp>

namespace commands {

using CheckPipeline = typename rfl::internal::define_named_tuple<
    rfl::Field<"type_", rfl::Literal<"Pipeline.check">>,
    rfl::Field<"name_", std::string>, DataFramesOrViews>::type;

}  // namespace commands

#endif  // COMMANDS_CHECK_PIPELINE_HPP_
