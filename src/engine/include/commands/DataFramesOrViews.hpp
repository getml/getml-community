// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATA_FRAMES_OR_VIEW_HPP_
#define COMMANDS_DATA_FRAMES_OR_VIEW_HPP_

#include <optional>
#include <vector>

#include "commands/DataFrameOrView.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"

namespace commands {

/// The commands to retrieve data frames or view,
/// used by many commands related to the pipelines.
using DataFramesOrViews = fct::NamedTuple<
    fct::Field<"population_df_", DataFrameOrView>,
    fct::Field<"peripheral_dfs_", std::vector<DataFrameOrView>>,
    fct::Field<"validation_df_", std::optional<DataFrameOrView>>>;

}  // namespace commands

#endif  // COMMANDS_DATA_FRAMES_OR_VIEW_HPP_