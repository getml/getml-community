// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FITPREPROCESSORSPARAMS_HPP_
#define ENGINE_PIPELINES_FITPREPROCESSORSPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <memory>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "engine/dependency/dependency.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace pipelines {

using FitPreprocessorsParams = fct::NamedTuple<

    /// The Encoding used for the categories.
    fct::Field<"categories_", fct::Ref<containers::Encoding>>,

    /// Contains all of the names of all data frames or views needed for fitting
    /// the pipeline.
    fct::Field<"cmd_", commands::DataFramesOrViews>,

    /// Logs the progress.
    fct::Field<"logger_", std::shared_ptr<const communication::Logger>>,

    /// The peripheral tables.
    fct::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>,

    /// The population table.
    fct::Field<"population_df_", containers::DataFrame>,

    /// The dependency tracker for the preprocessors.
    fct::Field<"preprocessor_tracker_",
               fct::Ref<dependency::PreprocessorTracker>>,

    /// Output: The socket with which we communicate.
    fct::Field<"socket_", Poco::Net::StreamSocket*>>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPREPROCESSORSPARAMS_HPP_

