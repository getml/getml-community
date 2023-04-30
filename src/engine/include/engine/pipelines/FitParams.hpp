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
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace pipelines {

using FitParams = fct::NamedTuple<

    /// The Encoding used for the categories.
    fct::Field<"categories_", fct::Ref<containers::Encoding>>,

    /// Contains all of the names of all data frames or views needed for fitting
    /// the pipeline.
    fct::Field<"cmd_", commands::DataFramesOrViews>,

    /// Contains all of the data frames - we need this, because it might be
    /// possible that the features are retrieved.
    fct::Field<"data_frames_", std::map<std::string, containers::DataFrame>>,

    /// Keeps track of the data frames and their fingerprints.
    fct::Field<"data_frame_tracker_", dependency::DataFrameTracker>,

    /// The dependency tracker for the feature learners.
    fct::Field<"fe_tracker_", fct::Ref<dependency::FETracker>>,

    /// The fingerprints of the feature selectors used for fitting.
    fct::Field<"fs_fingerprints_",
               fct::Ref<const std::vector<commands::Fingerprint>>>,

    /// Logs the progress.
    fct::Field<"logger_", std::shared_ptr<const communication::Logger>>,

    /// The peripheral tables.
    fct::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>,

    /// The population table.
    fct::Field<"population_df_", containers::DataFrame>,

    /// The dependency tracker for the predictors.
    fct::Field<"pred_tracker_", fct::Ref<dependency::PredTracker>>,

    /// The dependency tracker for the preprocessors.
    fct::Field<"preprocessor_tracker_",
               fct::Ref<dependency::PreprocessorTracker>>,

    /// The population table used for validation (only relevant for
    /// early stopping).
    fct::Field<"validation_df_", std::optional<containers::DataFrame>>,

    /// Output: The socket with which we communicate.
    fct::Field<"socket_", Poco::Net::StreamSocket*>>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPARAMS_HPP_
