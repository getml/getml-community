// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_MONITORSUMMARY_HPP_
#define CONTAINERS_MONITORSUMMARY_HPP_

#include <cstddef>
#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <string>
#include <vector>

#include "containers/Float.hpp"

namespace containers {

/// The summary for the monitor.
using MonitorSummary = rfl::NamedTuple<
    rfl::Field<"categorical_", std::vector<std::string>>,
    rfl::Field<"categorical_units_", std::vector<std::string>>,
    rfl::Field<"join_keys_", std::vector<std::string>>,
    rfl::Field<"name_", std::string>, rfl::Field<"num_categorical_", size_t>,
    rfl::Field<"num_join_keys_", size_t>, rfl::Field<"num_numerical_", size_t>,
    rfl::Field<"num_rows_", size_t>, rfl::Field<"num_targets_", size_t>,
    rfl::Field<"num_text_", size_t>, rfl::Field<"num_time_stamps_", size_t>,
    rfl::Field<"num_unused_floats_", size_t>,
    rfl::Field<"num_unused_strings_", size_t>,
    rfl::Field<"numerical_", std::vector<std::string>>,
    rfl::Field<"numerical_units_", std::vector<std::string>>,
    rfl::Field<"size_", Float>,
    rfl::Field<"targets_", std::vector<std::string>>,
    rfl::Field<"text_", std::vector<std::string>>,
    rfl::Field<"text_units_", std::vector<std::string>>,
    rfl::Field<"time_stamps_", std::vector<std::string>>,
    rfl::Field<"time_stamp_units_", std::vector<std::string>>,
    rfl::Field<"unused_floats_", std::vector<std::string>>,
    rfl::Field<"unused_float_units_", std::vector<std::string>>,
    rfl::Field<"unused_strings_", std::vector<std::string>>,
    rfl::Field<"unused_string_units_", std::vector<std::string>>>;

}  // namespace containers

#endif  // CONTAINERS_MONITORSUMMARY_HPP_
