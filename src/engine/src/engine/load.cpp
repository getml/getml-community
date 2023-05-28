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
#include "helpers/Loader.hpp"
#include "metrics/metrics.hpp"

namespace engine {
namespace pipelines {
namespace load {

Pipeline load(const std::string& _path) {
  const auto obj =
      helpers::Loader::load_from_json<fct::Ref<const commands::Pipeline>>(
          _path + "obj.json");

  const auto scores =
      helpers::Loader::load_from_json<fct::Ref<const metrics::Scores>>(
          _path + "scores.json");

  const auto pipeline_json =
      helpers::Loader::load_from_json<PipelineJSON>(_path + "pipeline.json");

  return Pipeline(obj)
      .with_scores(scores)
      .with_creation_time(pipeline_json.get<"creation_time_">())
      .with_allow_http(pipeline_json.get<"allow_http_">());
}

}  // namespace load
}  // namespace pipelines
}  // namespace engine

