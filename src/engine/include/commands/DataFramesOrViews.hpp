// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATA_FRAMES_OR_VIEW_HPP_
#define COMMANDS_DATA_FRAMES_OR_VIEW_HPP_

#include "commands/DataFrameOrView.hpp"

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>

#include <optional>
#include <vector>

namespace commands {

/// The commands to retrieve data frames or view,
/// used by many commands related to the pipelines.
struct DataFramesOrViews {
  rfl::Field<"population_df_", DataFrameOrView> population_df;
  rfl::Field<"peripheral_dfs_", std::vector<DataFrameOrView>> peripheral_dfs;
  rfl::Field<"validation_df_", std::optional<DataFrameOrView>> validation_df;
};

}  // namespace commands

#endif  // COMMANDS_DATA_FRAMES_OR_VIEW_HPP_
