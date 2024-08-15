// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_PIPELINEJSON_HPP_
#define ENGINE_PIPELINES_PIPELINEJSON_HPP_

#include <rfl/Field.hpp>
#include <rfl/Flatten.hpp>
#include <rfl/Ref.hpp>
#include <string>
#include <vector>

#include "engine/pipelines/Fingerprints.hpp"
#include "helpers/Schema.hpp"

namespace engine {
namespace pipelines {

struct PipelineJSON {
  /// The fingerprints required for the fitted pipeline.
  rfl::Flatten<Fingerprints> fingerprints;

  /// Whether the pipeline is allowed to handle HTTP requests.
  rfl::Field<"allow_http_", bool> allow_http;

  /// Date and time of creation, expressed as a string
  rfl::Field<"creation_time_", std::string> creation_time;

  /// The schema of the peripheral tables as they are inserted into the
  /// feature learners.
  rfl::Field<"modified_peripheral_schema_",
             rfl::Ref<const std::vector<helpers::Schema>>>
      modified_peripheral_schema;

  /// The schema of the population as it is inserted into the feature
  /// learners.
  rfl::Field<"modified_population_schema_", rfl::Ref<const helpers::Schema>>
      modified_population_schema;

  /// The schema of the peripheral tables as they are originally passed.
  rfl::Field<"peripheral_schema_", rfl::Ref<const std::vector<helpers::Schema>>>
      peripheral_schema;

  /// The schema of the population table as originally passed.
  rfl::Field<"population_schema_", rfl::Ref<const helpers::Schema>>
      population_schema;

  /// The names of the targets.
  rfl::Field<"targets_", std::vector<std::string>> targets;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_PIPELINEJSON_HPP_
