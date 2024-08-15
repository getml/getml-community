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
#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <rfl/named_tuple_t.hpp>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "commands/Fingerprint.hpp"
#include "containers/DataFrame.hpp"
#include "containers/Encoding.hpp"
#include "engine/dependency/DataFrameTracker.hpp"
#include "engine/dependency/FETracker.hpp"
#include "engine/dependency/PredTracker.hpp"
#include "engine/dependency/PreprocessorTracker.hpp"

namespace engine {
namespace pipelines {

struct FitParams {
  /// The Encoding used for the categories.
  rfl::Field<"categories_", rfl::Ref<containers::Encoding>> categories;

  /// Contains all of the names of all data frames or views needed for fitting
  /// the pipeline.
  rfl::Field<"cmd_", commands::DataFramesOrViews> cmd;

  /// Contains all of the data frames - we need this, because it might be
  /// possible that the features are retrieved.
  rfl::Field<"data_frames_", std::map<std::string, containers::DataFrame>>
      data_frames;

  /// Keeps track of the data frames and their fingerprints.
  rfl::Field<"data_frame_tracker_", dependency::DataFrameTracker>
      data_frame_tracker;

  /// The dependency tracker for the feature learners.
  rfl::Field<"fe_tracker_", rfl::Ref<dependency::FETracker>> fe_tracker;

  /// The fingerprints of the feature selectors used for fitting.
  rfl::Field<"fs_fingerprints_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      fs_fingerprints;

  /// Logs the progress.
  rfl::Field<"logger_", std::shared_ptr<const communication::Logger>> logger;

  /// The peripheral tables.
  rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>
      peripheral_dfs;

  /// The population table.
  rfl::Field<"population_df_", containers::DataFrame> population_df;

  /// The dependency tracker for the predictors.
  rfl::Field<"pred_tracker_", rfl::Ref<dependency::PredTracker>> pred_tracker;

  /// The dependency tracker for the preprocessors.
  rfl::Field<"preprocessor_tracker_", rfl::Ref<dependency::PreprocessorTracker>>
      preprocessor_tracker;

  /// The population table used for validation (only relevant for
  /// early stopping).
  rfl::Field<"validation_df_", std::optional<containers::DataFrame>>
      validation_df;

  /// Output: The socket with which we communicate.
  rfl::Field<"socket_", Poco::Net::StreamSocket*> socket;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPARAMS_HPP_
