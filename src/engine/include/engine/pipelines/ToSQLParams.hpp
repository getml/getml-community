// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_TOSQLPARAMS_HPP_
#define ENGINE_PIPELINES_TOSQLPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <optional>
#include <vector>

#include "containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace engine {
namespace pipelines {

using ToSQLParams = rfl::NamedTuple<
    /// Encodes the categories.
    rfl::Field<"categories_", const helpers::StringIterator>,

    /// The fitted pipeline.
    rfl::Field<"fitted_", FittedPipeline>,

    /// Whether we want to transpile the full pipeline or just the features.
    rfl::Field<"full_pipeline_", bool>,

    /// The underlying pipeline,
    rfl::Field<"pipeline_", Pipeline>,

    /// If the feature is longer than the threshold, it will not be sent. This
    /// is to prevent the iPython notebook from overflowing and/or unexpectedly
    /// high memory usage in Python.
    rfl::Field<"size_threshold_", std::optional<size_t>>,

    /// Whether we want to include the targets in the transpiled code (needed to
    /// generate a training set)
    rfl::Field<"targets_", bool>,

    /// The parameters required by the transpilation package.
    rfl::Field<"transpilation_params_", transpilation::TranspilationParams>>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TOSQLPARAMS_HPP_
