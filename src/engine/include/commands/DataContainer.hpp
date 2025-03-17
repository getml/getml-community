// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATACONTAINER_HPP_
#define COMMANDS_DATACONTAINER_HPP_

#include "commands/DataFrameOrView.hpp"

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/define_named_tuple.hpp>

#include <map>
#include <string>
#include <variant>
#include <vector>

namespace commands {

using DataContainerPeripheralType =
    std::variant<DataFrameOrView, std::vector<DataFrameOrView>,
                 std::map<std::string, DataFrameOrView>>;

using DataContainer = rfl::define_named_tuple_t<
    rfl::Field<"id_", std::string>, rfl::Field<"deep_copy_", bool>,
    rfl::Field<"last_change_", std::string>,
    rfl::Field<"frozen_time_", std::optional<std::string>>,
    rfl::Field<"subsets_", std::map<std::string, DataFrameOrView>>,
    rfl::Field<"peripheral_", DataContainerPeripheralType>>;

}  // namespace commands

#endif
