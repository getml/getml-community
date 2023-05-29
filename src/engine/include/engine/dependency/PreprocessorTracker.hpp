// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_DEPENDENCY_PREPROCESSORTRACKER_HPP_
#define ENGINE_DEPENDENCY_PREPROCESSORTRACKER_HPP_

#include "engine/dependency/Tracker.hpp"
#include "engine/preprocessors/preprocessors.hpp"

namespace engine {
namespace dependency {

typedef Tracker<preprocessors::Preprocessor> PreprocessorTracker;

}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_PREPROCESSORTRACKER_HPP_
