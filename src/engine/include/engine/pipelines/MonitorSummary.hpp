// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_MONITORSUMMARY_HPP_
#define ENGINE_PIPELINES_MONITORSUMMARY_HPP_

#include "commands/Pipeline.hpp"
#include "helpers/Schema.hpp"

#include <rfl/Field.hpp>
#include <rfl/Flatten.hpp>
#include <rfl/Ref.hpp>

#include <cstddef>
#include <string>
#include <variant>
#include <vector>

namespace engine {
namespace pipelines {

struct MonitorSummaryNotFitted {
  rfl::Flatten<commands::Pipeline> pipeline;
  rfl::Field<"allow_http_", bool> allow_http;
  rfl::Field<"creation_time_", std::string> creation_time;
};

struct MonitorSummaryFitted {
  rfl::Flatten<MonitorSummaryNotFitted> not_fitted;
  rfl::Field<"num_features_", size_t> num_features;
  rfl::Field<"peripheral_schema_", rfl::Ref<const std::vector<helpers::Schema>>>
      peripheral_schema;
  rfl::Field<"population_schema_", rfl::Ref<const helpers::Schema>>
      population_schema;
  rfl::Field<"targets_", std::vector<std::string>> targets;
};

using MonitorSummary =
    std::variant<MonitorSummaryFitted, MonitorSummaryNotFitted>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_MONITORSUMMARY_HPP_
