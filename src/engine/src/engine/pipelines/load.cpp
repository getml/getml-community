// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/load.hpp"

#include "commands/Pipeline.hpp"
#include "engine/pipelines/PipelineJSON.hpp"
#include "engine/pipelines/load_fitted.hpp"
#include "helpers/Loader.hpp"

#include <rfl/Field.hpp>

#include <string>

namespace engine {
namespace pipelines {
namespace load {

Pipeline load(const std::string& _path,
              const dependency::PipelineTrackers& _pipeline_trackers) {
  const auto obj =
      helpers::Loader::load<rfl::Ref<const commands::Pipeline>>(_path + "obj");

  const auto scores =
      helpers::Loader::load<rfl::Ref<const metrics::Scores>>(_path + "scores");

  const auto pipeline_json =
      helpers::Loader::load<PipelineJSON>(_path + "pipeline");

  const auto p = Pipeline(obj)
                     .with_scores(scores)
                     .with_creation_time(pipeline_json.creation_time())
                     .with_allow_http(pipeline_json.allow_http());

  return pipelines::load_fitted::load_fitted(_path, p, _pipeline_trackers);
}

}  // namespace load
}  // namespace pipelines
}  // namespace engine
