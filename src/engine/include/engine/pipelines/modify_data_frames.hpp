// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_MODIFYDATAFRAMES_HPP_
#define ENGINE_PIPELINES_MODIFYDATAFRAMES_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "commands/DataModel.hpp"
#include "containers/DataFrame.hpp"
#include "engine/Float.hpp"
#include "engine/Int.hpp"

namespace engine {
namespace pipelines {
namespace modify_data_frames {

/// Adds a constant join keys. This is needed for when the user has not
/// explicitly passed a join key.
void add_join_keys(const commands::DataModel& _data_model,
                   const std::vector<std::string>& _peripheral_names,
                   const std::optional<std::string>& _temp_dir,
                   containers::DataFrame* _population_df,
                   std::vector<containers::DataFrame>* _peripheral_dfs,
                   std::shared_ptr<containers::Encoding> _encoding = nullptr);

/// Extracts upper time stamps from the memory parameter. (Memory is just
/// syntactic sugar for upper time stamps. The feature learners don't know
/// about this concept).
void add_time_stamps(const commands::DataModel& _data_model,
                     const std::vector<std::string>& _peripheral_names,
                     containers::DataFrame* _population_df,
                     std::vector<containers::DataFrame>* _peripheral_dfs);

}  // namespace modify_data_frames
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_DATAFRAMEMODIFIER_HPP_
