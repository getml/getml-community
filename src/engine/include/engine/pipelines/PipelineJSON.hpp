// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_PIPELINEJSON_HPP_
#define ENGINE_PIPELINES_PIPELINEJSON_HPP_

#include <string>
#include <vector>

#include "engine/pipelines/Fingerprints.hpp"
#include "helpers/Schema.hpp"
#include "rfl/Field.hpp"
#include "rfl/Ref.hpp"
#include "rfl/define_named_tuple.hpp"

namespace engine {
namespace pipelines {

using PipelineJSON = rfl::define_named_tuple_t<

    /// The fingerprints required for the fitted pipeline.
    Fingerprints,

    /// Whether the pipeline is allowed to handle HTTP requests.
    rfl::Field<"allow_http_", bool>,

    /// Date and time of creation, expressed as a string
    rfl::Field<"creation_time_", std::string>,

    /// The schema of the peripheral tables as they are inserted into the
    /// feature learners.
    rfl::Field<"modified_peripheral_schema_",
               rfl::Ref<const std::vector<helpers::Schema>>>,

    /// The schema of the population as it is inserted into the feature
    /// learners.
    rfl::Field<"modified_population_schema_", rfl::Ref<const helpers::Schema>>,

    /// The schema of the peripheral tables as they are originally passed.
    rfl::Field<"peripheral_schema_",
               rfl::Ref<const std::vector<helpers::Schema>>>,

    /// The schema of the population table as originally passed.
    rfl::Field<"population_schema_", rfl::Ref<const helpers::Schema>>,

    /// The names of the targets.
    rfl::Field<"targets_", std::vector<std::string>>>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_PIPELINEJSON_HPP_

