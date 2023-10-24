// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_CHECK_HPP_
#define ENGINE_PIPELINES_CHECK_HPP_

#include "engine/pipelines/CheckParams.hpp"
#include "engine/pipelines/Pipeline.hpp"

namespace engine {
namespace pipelines {
namespace check {

/// Checks the data model for any inconsistencies.
void check(const Pipeline& _pipeline, const CheckParams& _params);

}  // namespace check
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_CHECK_HPP_
