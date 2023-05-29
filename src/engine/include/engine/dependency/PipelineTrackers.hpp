// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_DEPENDENCY_PIPELINETRACKERS_HPP_
#define ENGINE_DEPENDENCY_PIPELINETRACKERS_HPP_

#include "engine/dependency/DataFrameTracker.hpp"
#include "engine/dependency/FETracker.hpp"
#include "engine/dependency/PredTracker.hpp"
#include "engine/dependency/PreprocessorTracker.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace dependency {

using PipelineTrackers = fct::NamedTuple<
    fct::Field<"data_frame_tracker_", fct::Ref<DataFrameTracker>>,
    fct::Field<"fe_tracker_", fct::Ref<FETracker>>,
    fct::Field<"pred_tracker_", fct::Ref<PredTracker>>,
    fct::Field<"preprocessor_tracker_", fct::Ref<PreprocessorTracker>>>;

}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_PIPELINETRACKERS_HPP_
