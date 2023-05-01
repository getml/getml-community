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
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "featurelearners/Int.hpp"

namespace featurelearners {

using FitParams = fct::NamedTuple<

    /// Contains all of the names of all data frames or views needed for fitting
    /// the pipeline.
    fct::Field<"cmd_", commands::DataFramesOrViews>,

    /// The peripheral tables.
    fct::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>,

    /// The population table.
    fct::Field<"population_df_", containers::DataFrame>,

    /// The prefix, used to identify the feature learner.
    fct::Field<"prefix_", std::string>,

    /// Logs the progress.
    fct::Field<"socket_logger_",
               std::shared_ptr<const communication::SocketLogger>>,

    /// The prefix, used to identify the feature learner.
    fct::Field<"temp_dir_", std::optional<std::string>>>;

}  // namespace featurelearners

#endif  // FEATURELEARNERS_FITPARAMS_HPP_

