// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_LOAD_HPP_
#define ENGINE_PIPELINES_LOAD_HPP_

#include "engine/dependency/PipelineTrackers.hpp"
#include "engine/pipelines/Pipeline.hpp"

namespace engine {
namespace pipelines {
namespace load {

/// Loads the pipeline from the hard disk.
Pipeline load(const std::string& _path,
              const dependency::PipelineTrackers& _pipeline_trackers);

}  // namespace load
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_LOAD_HPP_
