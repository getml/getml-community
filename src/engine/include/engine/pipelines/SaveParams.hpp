// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_SAVEPARAMS_HPP_
#define ENGINE_PIPELINES_SAVEPARAMS_HPP_

#include <memory>
#include <optional>
#include <vector>

#include "containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "helpers/Saver.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace engine {
namespace pipelines {

using SaveParams = rfl::NamedTuple<

    /// Encodes the categories.
    rfl::Field<"categories_", const helpers::StringIterator>,

    /// The fitted pipeline.
    rfl::Field<"fitted_", FittedPipeline>,

    /// The file format to use.
    rfl::Field<"format_", typename helpers::Saver::Format>,

    /// The name of the pipeline to be save.
    rfl::Field<"name_", std::string>,

    /// The path in which to save the final result.
    rfl::Field<"path_", std::string>,

    /// The underlying pipeline,
    rfl::Field<"pipeline_", Pipeline>,

    /// A path to a temporary directory.
    rfl::Field<"temp_dir_", std::string> >;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_SAVEPARAMS_HPP_
