// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_LOAD_HPP_
#define ENGINE_PIPELINES_LOAD_HPP_

#include <Poco/JSON/Object.h>

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "engine/dependency/dependency.hpp"
#include "engine/pipelines/Fingerprints.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "engine/pipelines/PipelineJSON.hpp"
#include "engine/pipelines/Predictors.hpp"
#include "metrics/metrics.hpp"

namespace engine {
namespace pipelines {

class Load {
 public:
  /// Loads the pipeline from the hard disk.
  static Pipeline load(
      const std::string& _path,
      const std::shared_ptr<dependency::FETracker> _fe_tracker,
      const std::shared_ptr<dependency::PredTracker> _pred_tracker,
      const std::shared_ptr<dependency::PreprocessorTracker>
          _preprocessor_tracker);

 private:
  /// Loads the feature learners.
  static std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>
  load_feature_learners(
      const std::string& _path,
      const std::shared_ptr<dependency::FETracker> _fe_tracker,
      const PipelineJSON& _pipeline_json, const Pipeline& _pipeline);

  /// Loads the feature selectors.
  static Predictors load_feature_selectors(
      const std::string& _path,
      const std::shared_ptr<dependency::PredTracker> _pred_tracker,
      const fct::Ref<const predictors::PredictorImpl>& _feature_selector_impl,
      const PipelineJSON& _pipeline_json, const Pipeline& _pipeline);

  /// Loads fingerprints for all components of the pipeline.
  static Fingerprints load_fingerprints(
      const Poco::JSON::Object& _pipeline_json);

  /// Loads the impls for the feature selectors and predictors.
  static std::pair<fct::Ref<const predictors::PredictorImpl>,
                   fct::Ref<const predictors::PredictorImpl>>
  load_impls(const std::string& _path);

  /// Loads a generic JSON object.
  static Poco::JSON::Object load_json_obj(const std::string& _fname);

  /// Loads the pipeline json (which records the original command passed by the
  /// user)
  static PipelineJSON load_pipeline_json(const std::string& _path);

  /// Loads the predictors.
  static Predictors load_predictors(
      const std::string& _path,
      const std::shared_ptr<dependency::PredTracker> _pred_tracker,
      const fct::Ref<const predictors::PredictorImpl>& _predictor_impl,
      const PipelineJSON& _pipeline_json, const Pipeline& _pipeline);

  /// Loads the preprocessors.
  static std::vector<fct::Ref<const preprocessors::Preprocessor>>
  load_preprocessors(const std::string& _path,
                     const std::shared_ptr<dependency::PreprocessorTracker>
                         _preprocessor_tracker,
                     const PipelineJSON& _pipeline_json,
                     const Pipeline& _pipeline);
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_LOAD_HPP_
