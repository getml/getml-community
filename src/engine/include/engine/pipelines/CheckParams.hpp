// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_CHECKPARAMS_HPP_
#define ENGINE_PIPELINES_CHECKPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <optional>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace engine {
namespace pipelines {

using CheckParams = rfl::NamedTuple<

    /// The Encoding used for the categories.
    rfl::Field<"categories_", rfl::Ref<containers::Encoding>>,

    /// Contains all of the names of all data frames or views needed for the
    /// check.
    rfl::Field<"cmd_", commands::DataFramesOrViews>,

    /// Logs the progress.
    rfl::Field<"logger_", std::shared_ptr<const communication::Logger>>,

    /// The peripheral tables.
    rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>,

    /// The population table.
    rfl::Field<"population_df_", containers::DataFrame>,

    /// The dependency tracker for the preprocessors.
    rfl::Field<"preprocessor_tracker_",
               rfl::Ref<dependency::PreprocessorTracker>>,

    /// Tracks the warnings to be shown in the Python API.
    rfl::Field<"warning_tracker_", rfl::Ref<dependency::WarningTracker>>,

    /// Output: The socket with which we communicate.
    rfl::Field<"socket_", Poco::Net::StreamSocket*>>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_CHECKPARAMS_HPP_
