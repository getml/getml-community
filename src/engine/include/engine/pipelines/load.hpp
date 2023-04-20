// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_LOAD_HPP_
#define ENGINE_PIPELINES_LOAD_HPP_

#include <memory>

#include "engine/dependency/FETracker.hpp"
#include "engine/dependency/PredTracker.hpp"
#include "engine/dependency/PreprocessorTracker.hpp"
#include "engine/pipelines/Pipeline.hpp"

namespace engine {
namespace pipelines {
namespace load {

/// Loads the pipeline from the hard disk.
Pipeline load(const std::string& _path,
              const std::shared_ptr<dependency::FETracker> _fe_tracker,
              const std::shared_ptr<dependency::PredTracker> _pred_tracker,
              const std::shared_ptr<dependency::PreprocessorTracker>
                  _preprocessor_tracker);

}  // namespace load
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_LOAD_HPP_
