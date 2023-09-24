// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_MAKEFEATURESPARAMS_HPP_
#define ENGINE_PIPELINES_MAKEFEATURESPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "commands/Fingerprint.hpp"
#include "commands/Predictor.hpp"
#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "engine/pipelines/TransformParams.hpp"
#include "rfl/Ref.hpp"

namespace engine {
namespace pipelines {

struct MakeFeaturesParams {
  /// The Encoding used for the categories.
  rfl::Field<"categories_", rfl::Ref<containers::Encoding>> categories;

  /// Contains all of the names of all data frames or views needed for fitting
  /// the pipeline.
  rfl::Field<"cmd_", commands::DataFramesOrViews> cmd;

  /// Keeps track of the data frames and their fingerprints.
  rfl::Field<"data_frame_tracker_", dependency::DataFrameTracker>
      data_frame_tracker;

  /// The depedencies of the predictors.
  rfl::Field<"dependencies_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      dependencies;

  /// Logs the progress.
  rfl::Field<"logger_", std::shared_ptr<const communication::Logger>> logger;

  /// The peripheral tables, without staging, as they were passed.
  rfl::Field<"original_peripheral_dfs_", std::vector<containers::DataFrame>>
      original_peripheral_dfs;

  /// The population table, without staging, as it was passed.
  rfl::Field<"original_population_df_", containers::DataFrame>
      original_population_df;

  /// The peripheral tables.
  rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>
      peripheral_dfs;

  /// The population table.
  rfl::Field<"population_df_", containers::DataFrame> population_df;

  /// Pimpl for the predictors.
  rfl::Field<"predictor_impl_", rfl::Ref<const predictors::PredictorImpl>>
      predictor_impl;

  /// Output: The autofeatures to be generated.
  rfl::Field<"autofeatures_", containers::NumericalFeatures*> autofeatures;

  /// Output: The socket with which we communicate.
  rfl::Field<"socket_", Poco::Net::StreamSocket*> socket;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_MAKEFEATURESPARAMS_HPP_
