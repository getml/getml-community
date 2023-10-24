// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATACONTAINER_HPP_
#define COMMANDS_DATACONTAINER_HPP_

#include <map>
#include <string>
#include <variant>
#include <vector>

#include "commands/DataFrameOrView.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/define_named_tuple.hpp"

namespace commands {

using DataContainerPeripheralType =
    std::variant<DataFrameOrView, std::vector<DataFrameOrView>,
                 std::map<std::string, DataFrameOrView>>;

using DataContainerBase =
    fct::NamedTuple<fct::Field<"id_", std::string>,
                    fct::Field<"deep_copy_", bool>,
                    fct::Field<"last_change_", std::string>,
                    fct::Field<"frozen_time_", std::string>>;

using DataContainerWithSplit = fct::define_named_tuple_t<
    fct::Field<"population_", DataFrameOrView>,
    fct::Field<"split_", commands::StringColumnOrStringColumnView>,
    fct::Field<"peripheral_", DataContainerPeripheralType>, DataContainerBase>;

using DataContainerWithSubsets = fct::define_named_tuple_t<
    fct::Field<"subsets_", std::map<std::string, DataFrameOrView>>,
    fct::Field<"peripheral_", DataContainerPeripheralType>, DataContainerBase>;

using DataContainer =
    std::variant<DataContainerWithSplit, DataContainerWithSubsets>;

}  // namespace commands

#endif
