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
#include "containers/containers.hpp"
#include "engine/communication/communication.hpp"
#include "engine/dependency/dependency.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/define_named_tuple.hpp"

namespace engine {
namespace pipelines {

struct TransformParams {
  using CmdType = fct::define_named_tuple_t<commands::DataFramesOrViews,
                                            fct::Field<"predict_", bool>,
                                            fct::Field<"score_", bool> >;

  static constexpr const char* FEATURE_SELECTOR = "feature selector";
  static constexpr const char* PREDICTOR = "predictor";

  /// The categorical encoding.
  const fct::Ref<containers::Encoding> categories_;

  /// The command used.
  const CmdType cmd_;

  /// Contains all of the data frames - we need this, because it might be
  /// possible that the features are retrieved.
  const std::map<std::string, containers::DataFrame> data_frames_;

  /// Keeps track of the data frames and their fingerprints.
  const dependency::DataFrameTracker data_frame_tracker_;

  /// Logs the progress.
  const std::shared_ptr<const communication::Logger> logger_;

  /// The peripheral tables, without staging, as they were passed.
  const std::vector<containers::DataFrame> original_peripheral_dfs_;

  /// The population table, without staging, as it was passed.
  const containers::DataFrame original_population_df_;

  /// Output: The socket with which we communicate.
  Poco::Net::StreamSocket* const socket_;
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TRANSFORMPARAMS_HPP_
