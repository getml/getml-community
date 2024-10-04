// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_STAGING_HPP_
#define ENGINE_PIPELINES_STAGING_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "containers/DataFrame.hpp"
#include "engine/Float.hpp"
#include "engine/Int.hpp"

namespace engine {
namespace pipelines {
namespace staging {

/// Parses the joined names to execute the many-to-one joins required in the
/// data model.
void join_tables(const std::vector<std::string>& _origin_peripheral_names,
                 const std::string& _joined_population_name,
                 const std::vector<std::string>& _joined_peripheral_names,
                 containers::DataFrame* _population_df,
                 std::vector<containers::DataFrame>* _peripheral_dfs);

}  // namespace staging
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_STAGING_HPP_

