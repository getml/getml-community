// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_MONITORSUMMARY_HPP_
#define CONTAINERS_MONITORSUMMARY_HPP_

#include <cstddef>
#include <string>
#include <vector>

#include "containers/Float.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"

namespace containers {

/// The summary for the monitor.
using MonitorSummary = fct::NamedTuple<
    fct::Field<"categorical_", std::vector<std::string>>,
    fct::Field<"categorical_units_", std::vector<std::string>>,
    fct::Field<"join_keys_", std::vector<std::string>>,
    fct::Field<"name_", std::string>, fct::Field<"num_categorical_", size_t>,
    fct::Field<"num_join_keys_", size_t>, fct::Field<"num_numerical_", size_t>,
    fct::Field<"num_rows_", size_t>, fct::Field<"num_targets_", size_t>,
    fct::Field<"num_text_", size_t>, fct::Field<"num_time_stamps_", size_t>,
    fct::Field<"num_unused_floats_", size_t>,
    fct::Field<"num_unused_strings_", size_t>,
    fct::Field<"numerical_", std::vector<std::string>>,
    fct::Field<"numerical_units_", std::vector<std::string>>,
    fct::Field<"size_", Float>,
    fct::Field<"targets_", std::vector<std::string>>,
    fct::Field<"text_", std::vector<std::string>>,
    fct::Field<"text_units_", std::vector<std::string>>,
    fct::Field<"time_stamps_", std::vector<std::string>>,
    fct::Field<"time_stamp_units_", std::vector<std::string>>,
    fct::Field<"unused_floats_", std::vector<std::string>>,
    fct::Field<"unused_float_units_", std::vector<std::string>>,
    fct::Field<"unused_strings_", std::vector<std::string>>,
    fct::Field<"unused_string_units_", std::vector<std::string>>>;

}  // namespace containers

#endif  // CONTAINERS_MONITORSUMMARY_HPP_
