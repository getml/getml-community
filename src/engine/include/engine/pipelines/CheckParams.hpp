// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_CHECKPARAMS_HPP_
#define ENGINE_PIPELINES_CHECKPARAMS_HPP_

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <optional>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"
#include "engine/dependency/dependency.hpp"

namespace engine {
namespace pipelines {

struct CheckParams {
  /// The Encoding used for the categories.
  const fct::Ref<containers::Encoding> categories_;

  /// The command used.
  const commands::DataFramesOrViews cmd_;

  /// Logs the progress.
  const std::shared_ptr<const communication::Logger> logger_;

  /// The peripheral tables.
  const std::vector<containers::DataFrame> peripheral_dfs_;

  /// The population table.
  const containers::DataFrame population_df_;

  /// The dependency tracker for the preprocessors.
  const fct::Ref<dependency::PreprocessorTracker> preprocessor_tracker_;

  /// Tracks the warnings to be shown in the Python API.
  const fct::Ref<dependency::WarningTracker> warning_tracker_;

  /// Output: The socket with which we communicate.
  Poco::Net::StreamSocket* const socket_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_CHECKPARAMS_HPP_
