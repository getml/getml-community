// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_TOSQLPARAMS_HPP_
#define ENGINE_PIPELINES_TOSQLPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <optional>
#include <vector>

#include "containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/Pipeline.hpp"

namespace engine {
namespace pipelines {

struct ToSQLParams {
  /// Encodes the categories.
  const helpers::StringIterator categories_;

  /// The fitted pipeline.
  const FittedPipeline fitted_;

  /// Whether we want to transpile the full pipeline or just the features.
  const bool full_pipeline_;

  /// The underlying pipeline,
  const Pipeline pipeline_;

  /// If the feature is longer than the threshold, it will not be sent. This is
  /// to prevent the iPython notebook from overflowing and/or unexpectedly high
  /// memory usage in Python.
  const std::optional<size_t> size_threshold_;

  /// Whether we want to include the targets in the transpiled code (needed to
  /// generate a training set)
  const bool targets_;

  /// The parameters required by the transpilation package.
  const transpilation::TranspilationParams transpilation_params_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TOSQLPARAMS_HPP_
