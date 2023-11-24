// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/load.hpp"

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "commands/Pipeline.hpp"
#include "engine/pipelines/PipelineJSON.hpp"
#include "engine/pipelines/load_fitted.hpp"
#include "fct/Field.hpp"
#include "helpers/Loader.hpp"
#include "metrics/metrics.hpp"

namespace engine {
namespace pipelines {
namespace load {

Pipeline load(const std::string& _path,
              const dependency::PipelineTrackers& _pipeline_trackers) {
  const auto obj =
      helpers::Loader::load_from_json<fct::Ref<const commands::Pipeline>>(
          _path + "obj.json");

  const auto scores =
      helpers::Loader::load_from_json<fct::Ref<const metrics::Scores>>(
          _path + "scores.json");

  const auto pipeline_json =
      helpers::Loader::load_from_json<PipelineJSON>(_path + "pipeline.json");

  const auto p = Pipeline(obj)
                     .with_scores(scores)
                     .with_creation_time(pipeline_json.get<"creation_time_">())
                     .with_allow_http(pipeline_json.get<"allow_http_">());

  return pipelines::load_fitted::load_fitted(_path, p, _pipeline_trackers);
}

}  // namespace load
}  // namespace pipelines
}  // namespace engine
