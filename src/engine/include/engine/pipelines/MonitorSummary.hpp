// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_MONITORSUMMARY_HPP_
#define ENGINE_PIPELINES_MONITORSUMMARY_HPP_

#include <cstddef>
#include <string>
#include <variant>
#include <vector>

#include "commands/Pipeline.hpp"
#include "helpers/Schema.hpp"
#include "rfl/Field.hpp"
#include "rfl/Ref.hpp"
#include "rfl/define_named_tuple.hpp"

namespace engine {
namespace pipelines {

using MonitorSummaryNotFitted =
    rfl::define_named_tuple_t<commands::Pipeline,
                              rfl::Field<"allow_http_", bool>,
                              rfl::Field<"creation_time_", std::string>>;

// TODO: Insert Scores object
using MonitorSummaryFitted = rfl::define_named_tuple_t<
    MonitorSummaryNotFitted, rfl::Field<"num_features_", size_t>,
    rfl::Field<"peripheral_schema_",
               rfl::Ref<const std::vector<helpers::Schema>>>,
    rfl::Field<"population_schema_", rfl::Ref<const helpers::Schema>>,
    rfl::Field<"targets_", std::vector<std::string>>>;

using MonitorSummary =
    std::variant<MonitorSummaryFitted, MonitorSummaryNotFitted>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_MONITORSUMMARY_HPP_
