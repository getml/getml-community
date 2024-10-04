// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATACONTAINER_HPP_
#define COMMANDS_DATACONTAINER_HPP_

#include <map>
#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/define_named_tuple.hpp>
#include <string>
#include <variant>
#include <vector>

#include "commands/DataFrameOrView.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"

namespace commands {

using DataContainerPeripheralType =
    std::variant<DataFrameOrView, std::vector<DataFrameOrView>,
                 std::map<std::string, DataFrameOrView>>;

using DataContainerBase =
    rfl::NamedTuple<rfl::Field<"id_", std::string>,
                    rfl::Field<"deep_copy_", bool>,
                    rfl::Field<"last_change_", std::string>,
                    rfl::Field<"frozen_time_", std::string>>;

using DataContainerWithSplit = rfl::define_named_tuple_t<
    rfl::Field<"population_", DataFrameOrView>,
    rfl::Field<"split_", commands::StringColumnOrStringColumnView>,
    rfl::Field<"peripheral_", DataContainerPeripheralType>, DataContainerBase>;

using DataContainerWithSubsets = rfl::define_named_tuple_t<
    rfl::Field<"subsets_", std::map<std::string, DataFrameOrView>>,
    rfl::Field<"peripheral_", DataContainerPeripheralType>, DataContainerBase>;

using DataContainer =
    std::variant<DataContainerWithSplit, DataContainerWithSubsets>;

}  // namespace commands

#endif
