// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_TRANSFORMPARAMS_HPP_
#define ENGINE_PIPELINES_TRANSFORMPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/define_named_tuple.hpp"

namespace engine {
namespace pipelines {

using TransformCmdType = fct::define_named_tuple_t<commands::DataFramesOrViews,
                                                   fct::Field<"predict_", bool>,
                                                   fct::Field<"score_", bool>>;

using TransformParams = fct::NamedTuple<

    /// The Encoding used for the categories.
    fct::Field<"categories_", fct::Ref<containers::Encoding>>,

    /// Contains all of the names of all data frames or views needed for fitting
    /// the pipeline.
    fct::Field<"cmd_", TransformCmdType>,

    /// Contains all of the data frames - we need this, because it might be
    /// possible that the features are retrieved.
    fct::Field<"data_frames_", std::map<std::string, containers::DataFrame>>,

    /// Keeps track of the data frames and their fingerprints.
    fct::Field<"data_frame_tracker_", dependency::DataFrameTracker>,

    /// Logs the progress.
    fct::Field<"logger_", std::shared_ptr<const communication::Logger>>,

    /// The peripheral tables.
    fct::Field<"original_peripheral_dfs_", std::vector<containers::DataFrame>>,

    /// The population table.
    fct::Field<"original_population_df_", containers::DataFrame>,

    /// Output: The socket with which we communicate.
    fct::Field<"socket_", Poco::Net::StreamSocket*>>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TRANSFORMPARAMS_HPP_
