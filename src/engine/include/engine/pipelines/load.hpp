// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_LOAD_HPP_
#define ENGINE_PIPELINES_LOAD_HPP_

#include "engine/pipelines/Pipeline.hpp"

namespace engine {
namespace pipelines {
namespace load {

/// Loads the pipeline from the hard disk.
Pipeline load(const std::string& _path);

}  // namespace load
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_LOAD_HPP_
