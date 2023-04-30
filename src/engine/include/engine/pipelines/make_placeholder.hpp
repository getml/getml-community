// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_MAKEPLACEHOLDER_HPP_
#define ENGINE_PIPELINES_MAKEPLACEHOLDER_HPP_

#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "commands/DataModel.hpp"
#include "engine/Float.hpp"
#include "fct/Ref.hpp"
#include "helpers/Placeholder.hpp"

namespace engine {
namespace pipelines {
namespace make_placeholder {

/// Creates the placeholder, including transforming memory into upper time
/// stamps.
fct::Ref<const helpers::Placeholder> make_placeholder(
    const commands::DataModel& _data_model, const std::string& _alias,
    const std::shared_ptr<size_t> _num_alias = nullptr,
    const bool _is_population = true);

/// Returns a list of all peripheral tables used in the placeholder.
std::vector<std::string> make_peripheral(
    const helpers::Placeholder& _placeholder);

/// Generates the name for the time stamp that is produced using
/// memory.
std::string make_ts_name(const std::string& _ts_used, const Float _diff);

}  // namespace make_placeholder
}  // namespace pipelines
}  // namespace engine

#endif
