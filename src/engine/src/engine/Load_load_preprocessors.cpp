// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/Load.hpp"
#include "engine/pipelines/fit.hpp"

namespace engine {
namespace pipelines {

std::vector<fct::Ref<const preprocessors::Preprocessor>>
Load::load_preprocessors(const std::string& _path,
                         const std::shared_ptr<dependency::PreprocessorTracker>
                             _preprocessor_tracker,
                         const PipelineJSON& _pipeline_json,
                         const Pipeline& _pipeline) {
  assert_true(_preprocessor_tracker);

  auto preprocessors = fit::init_preprocessors(
      _pipeline, _pipeline_json.get<"df_fingerprints_">());

  for (size_t i = 0; i < preprocessors.size(); ++i) {
    const auto& p = preprocessors.at(i);
    p->load(_path + "preprocessor-" + std::to_string(i) + ".json");
    _preprocessor_tracker->add(p);
  }

  return fit::to_const(preprocessors);
}

}  // namespace pipelines
}  // namespace engine

