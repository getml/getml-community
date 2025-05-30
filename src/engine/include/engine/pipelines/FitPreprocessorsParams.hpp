// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FITPREPROCESSORSPARAMS_HPP_
#define ENGINE_PIPELINES_FITPREPROCESSORSPARAMS_HPP_

#include "commands/DataFramesOrViews.hpp"
#include "communication/Logger.hpp"
#include "containers/DataFrame.hpp"
#include "containers/Encoding.hpp"
#include "engine/dependency/PreprocessorTracker.hpp"

#include <Poco/Net/StreamSocket.h>
#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>

#include <memory>
#include <vector>

namespace engine {
namespace pipelines {

struct FitPreprocessorsParams {
  /// The Encoding used for the categories.
  rfl::Field<"categories_", rfl::Ref<containers::Encoding>> categories;

  /// Contains all of the names of all data frames or views needed for fitting
  /// the pipeline.
  rfl::Field<"cmd_", commands::DataFramesOrViews> cmd;

  /// Logs the progress.
  rfl::Field<"logger_", std::shared_ptr<const communication::Logger>> logger;

  /// The peripheral tables.
  rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>
      peripheral_dfs;

  /// The population table.
  rfl::Field<"population_df_", containers::DataFrame> population_df;

  /// The dependency tracker for the preprocessors.
  rfl::Field<"preprocessor_tracker_", rfl::Ref<dependency::PreprocessorTracker>>
      preprocessor_tracker;

  /// Output: The socket with which we communicate.
  rfl::Field<"socket_", Poco::Net::StreamSocket*> socket;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPREPROCESSORSPARAMS_HPP_
