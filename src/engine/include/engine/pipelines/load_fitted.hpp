// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_LOADFITTED_HPP_
#define ENGINE_PIPELINES_LOADFITTED_HPP_

#include "engine/dependency/PipelineTrackers.hpp"
#include "engine/pipelines/Pipeline.hpp"

namespace engine {
namespace pipelines {
namespace load_fitted {

/// Loads the fitted pipeline from the hard disk.
Pipeline load_fitted(const std::string& _path, const Pipeline& _pipeline,
                     const dependency::PipelineTrackers& _pipeline_trackers);

}  // namespace load_fitted
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_LOADFITTED_HPP_
