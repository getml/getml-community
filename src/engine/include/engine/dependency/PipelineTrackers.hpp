// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_DEPENDENCY_PIPELINETRACKERS_HPP_
#define ENGINE_DEPENDENCY_PIPELINETRACKERS_HPP_

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>

#include "engine/dependency/DataFrameTracker.hpp"
#include "engine/dependency/FETracker.hpp"
#include "engine/dependency/PredTracker.hpp"
#include "engine/dependency/PreprocessorTracker.hpp"

namespace engine {
namespace dependency {

using PipelineTrackers = rfl::NamedTuple<
    rfl::Field<"data_frame_tracker_", rfl::Ref<DataFrameTracker>>,
    rfl::Field<"fe_tracker_", rfl::Ref<FETracker>>,
    rfl::Field<"pred_tracker_", rfl::Ref<PredTracker>>,
    rfl::Field<"preprocessor_tracker_", rfl::Ref<PreprocessorTracker>>>;

}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_PIPELINETRACKERS_HPP_
