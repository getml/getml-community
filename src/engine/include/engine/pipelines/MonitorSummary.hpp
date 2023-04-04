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
#include "fct/Field.hpp"
#include "fct/Ref.hpp"
#include "fct/define_named_tuple.hpp"
#include "helpers/Schema.hpp"

namespace engine {
namespace pipelines {

using MonitorSummaryNotFitted =
    fct::define_named_tuple_t<commands::Pipeline,
                              fct::Field<"allow_http_", bool>,
                              fct::Field<"creation_time_", std::string>>;

// TODO: Insert Scores object
using MonitorSummaryFitted = fct::define_named_tuple_t<
    MonitorSummaryNotFitted, fct::Field<"num_features_", size_t>,
    fct::Field<"peripheral_schema_",
               fct::Ref<const std::vector<helpers::Schema>>>,
    fct::Field<"population_schema_", fct::Ref<const helpers::Schema>>,
    fct::Field<"targets_", std::vector<std::string>>>;

using MonitorSummary =
    std::variant<MonitorSummaryFitted, MonitorSummaryNotFitted>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_MONITORSUMMARY_HPP_
