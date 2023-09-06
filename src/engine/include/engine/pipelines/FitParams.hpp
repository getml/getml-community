// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FITPARAMS_HPP_
#define ENGINE_PIPELINES_FITPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "commands/Fingerprint.hpp"
#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace engine {
namespace pipelines {

using FitParams = rfl::NamedTuple<

    /// The Encoding used for the categories.
    rfl::Field<"categories_", rfl::Ref<containers::Encoding>>,

    /// Contains all of the names of all data frames or views needed for fitting
    /// the pipeline.
    rfl::Field<"cmd_", commands::DataFramesOrViews>,

    /// Contains all of the data frames - we need this, because it might be
    /// possible that the features are retrieved.
    rfl::Field<"data_frames_", std::map<std::string, containers::DataFrame>>,

    /// Keeps track of the data frames and their fingerprints.
    rfl::Field<"data_frame_tracker_", dependency::DataFrameTracker>,

    /// The dependency tracker for the feature learners.
    rfl::Field<"fe_tracker_", rfl::Ref<dependency::FETracker>>,

    /// The fingerprints of the feature selectors used for fitting.
    rfl::Field<"fs_fingerprints_",
               rfl::Ref<const std::vector<commands::Fingerprint>>>,

    /// Logs the progress.
    rfl::Field<"logger_", std::shared_ptr<const communication::Logger>>,

    /// The peripheral tables.
    rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>,

    /// The population table.
    rfl::Field<"population_df_", containers::DataFrame>,

    /// The dependency tracker for the predictors.
    rfl::Field<"pred_tracker_", rfl::Ref<dependency::PredTracker>>,

    /// The dependency tracker for the preprocessors.
    rfl::Field<"preprocessor_tracker_",
               rfl::Ref<dependency::PreprocessorTracker>>,

    /// The population table used for validation (only relevant for
    /// early stopping).
    rfl::Field<"validation_df_", std::optional<containers::DataFrame>>,

    /// Output: The socket with which we communicate.
    rfl::Field<"socket_", Poco::Net::StreamSocket*>>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPARAMS_HPP_
