// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FEATURELEARNERS_FITPARAMS_HPP_
#define FEATURELEARNERS_FITPARAMS_HPP_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "featurelearners/Int.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"

namespace featurelearners {

using FitParams = rfl::NamedTuple<

    /// Contains all of the names of all data frames or views needed for fitting
    /// the pipeline.
    rfl::Field<"cmd_", commands::DataFramesOrViews>,

    /// The peripheral tables.
    rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>,

    /// The population table.
    rfl::Field<"population_df_", containers::DataFrame>,

    /// The prefix, used to identify the feature learner.
    rfl::Field<"prefix_", std::string>,

    /// Logs the progress.
    rfl::Field<"socket_logger_",
               std::shared_ptr<const communication::SocketLogger>>,

    /// The prefix, used to identify the feature learner.
    rfl::Field<"temp_dir_", std::optional<std::string>>>;

}  // namespace featurelearners

#endif  // FEATURELEARNERS_FITPARAMS_HPP_

