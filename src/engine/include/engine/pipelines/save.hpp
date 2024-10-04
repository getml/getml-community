// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_SAVE_HPP_
#define ENGINE_PIPELINES_SAVE_HPP_

#include "engine/pipelines/SaveParams.hpp"

namespace engine {
namespace pipelines {
namespace save {

/// Saves the pipeline.
void save(const SaveParams& _params);

}  // namespace save
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TOSQL_HPP_
