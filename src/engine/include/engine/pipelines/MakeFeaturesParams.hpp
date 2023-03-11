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
#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "engine/pipelines/TransformParams.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace pipelines {

struct MakeFeaturesParams {
  static constexpr const char* FEATURE_SELECTOR =
      TransformParams::FEATURE_SELECTOR;
  static constexpr const char* PREDICTOR = TransformParams::PREDICTOR;

  /// The categorical encoding.
  const fct::Ref<containers::Encoding> categories_;

  /// The command used.
  const commands::DataFramesOrViews cmd_;

  /// Keeps track of the data frames and their fingerprints.
  const dependency::DataFrameTracker data_frame_tracker_;

  /// The depedencies of the predictors.
  const fct::Ref<const std::vector<commands::Fingerprint>> dependencies_;

  /// Logs the progress.
  const std::shared_ptr<const communication::Logger> logger_;

  /// The peripheral tables, without staging, as they were passed.
  const std::vector<containers::DataFrame> original_peripheral_dfs_;

  /// The population table, without staging, as it was passed.
  const containers::DataFrame original_population_df_;

  /// The peripheral tables.
  const std::vector<containers::DataFrame> peripheral_dfs_;

  /// The population table.
  const containers::DataFrame population_df_;

  /// Impl for the predictors.
  const fct::Ref<const predictors::PredictorImpl> predictor_impl_;

  /// Output: The autofeatures to be generated.
  containers::NumericalFeatures* const autofeatures_ = nullptr;

  /// Output: The socket with which we communicate.
  Poco::Net::StreamSocket* const socket_;
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_MAKEFEATURESPARAMS_HPP_
