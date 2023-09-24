// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_TRANSFORMPARAMS_HPP_
#define ENGINE_PIPELINES_TRANSFORMPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/define_named_tuple.hpp"
#include "rfl/named_tuple_t.hpp"

namespace engine {
namespace pipelines {

struct TransformParams {
  struct Cmd {
    rfl::Flatten<commands::DataFramesOrViews> data_frames_or_views;
    rfl::Field<"predict_", bool> predict;
    rfl::Field<"score_", bool> score;
  };

  /// The Encoding used for the categories.
  rfl::Field<"categories_", rfl::Ref<containers::Encoding>> categories;

  /// Contains all of the names of all data frames or views needed for
  /// fitting the pipeline.
  rfl::Field<"cmd_", Cmd> cmd;

  /// Contains all of the data frames - we need this, because it might be
  /// possible that the features are retrieved.
  rfl::Field<"data_frames_", std::map<std::string, containers::DataFrame>>
      data_frames;

  /// Keeps track of the data frames and their fingerprints.
  rfl::Field<"data_frame_tracker_", dependency::DataFrameTracker>
      data_frame_tracker;

  /// Logs the progress.
  rfl::Field<"logger_", std::shared_ptr<const communication::Logger>> logger;

  /// The peripheral tables.
  rfl::Field<"original_peripheral_dfs_", std::vector<containers::DataFrame>>
      original_peripheral_dfs;

  /// The population table.
  rfl::Field<"original_population_df_", containers::DataFrame>
      original_population_df;

  /// Output: The socket with which we communicate.
  rfl::Field<"socket_", Poco::Net::StreamSocket*> socket;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TRANSFORMPARAMS_HPP_
