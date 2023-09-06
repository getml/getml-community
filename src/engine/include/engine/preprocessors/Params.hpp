// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_PARAMS_HPP_
#define ENGINE_PREPROCESSORS_PARAMS_HPP_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "communication/SocketLogger.hpp"
#include "containers/containers.hpp"
#include "helpers/Placeholder.hpp"
#include "logging/logging.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace engine {
namespace preprocessors {

using Params = rfl::NamedTuple<

    /// The Encoding used for the categories.
    rfl::Field<"categories_", rfl::Ref<containers::Encoding>>,

    /// Contains all of the names of all data frames or views needed for fitting
    /// the pipeline.
    rfl::Field<"cmd_", commands::DataFramesOrViews>,

    /// Logs the progress.
    rfl::Field<"logger_", std::shared_ptr<const communication::SocketLogger>>,

    /// The percentage at which we want the logging to begin.
    rfl::Field<"logging_begin_", size_t>,

    /// The percentage at which we want the logging to end.
    rfl::Field<"logging_end_", size_t>,

    /// The peripheral tables.
    rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>,

    /// The peripheral tables.
    rfl::Field<"peripheral_names_", std::vector<std::string>>,

    /// The peripheral tables.
    rfl::Field<"placeholder_", helpers::Placeholder>,

    /// The population table.
    rfl::Field<"population_df_", containers::DataFrame>>;

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_PARAMS_HPP_
