
// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_PIPELINE_HPP_
#define ENGINE_PIPELINES_PIPELINE_HPP_

#include <Poco/Net/StreamSocket.h>
#include <Poco/TemporaryFile.h>

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "commands/Pipeline.hpp"
#include "containers/containers.hpp"
#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/pipelines/CheckParams.hpp"
#include "engine/pipelines/FitParams.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/MonitorSummary.hpp"
#include "engine/pipelines/TransformParams.hpp"
#include "helpers/Placeholder.hpp"
#include "helpers/StringIterator.hpp"
#include "metrics/Scores.hpp"

namespace engine {
namespace pipelines {

class Pipeline {
 public:
  Pipeline(const fct::Ref<const commands::Pipeline>& _obj);

  Pipeline(const commands::Pipeline& _obj);

  ~Pipeline();

 public:
  /// Expresses the Pipeline in a form the monitor can understand.
  MonitorSummary to_monitor(const helpers::StringIterator& _categories,
                            const std::string& _name) const;

 private:
  // Helper function for the creation time.
  static std::string make_creation_time() {
    const auto now = Poco::LocalDateTime();
    return Poco::DateTimeFormatter::format(
        now, Poco::DateTimeFormat::SORTABLE_FORMAT);
  }

 public:
  /// Trivial (const) accessor
  bool allow_http() const { return allow_http_; }

  /// Trivial (const) accessor
  const auto& creation_time() const { return creation_time_; }

  /// Trivial (const) accessor
  const auto& fitted() const { return fitted_; }

  /// Trivial (const) accessor
  bool include_categorical() const { return include_categorical_; }

  /// Trivial (const) accessor
  const commands::Pipeline& obj() const { return *obj_; }

  /// Trivial (const) accessor
  const auto& scores() const { return *scores_; }

  /// Returns a new pipeline with a new value for allow_http_.
  Pipeline with_allow_http(const bool _allow_http) const {
    auto new_pipeline = *this;
    new_pipeline.allow_http_ = _allow_http;
    return new_pipeline;
  }

  /// Returns a new pipeline with a new value for creation_time_.
  Pipeline with_creation_time(const std::string& _creation_time) const {
    auto new_pipeline = *this;
    new_pipeline.creation_time_ = _creation_time;
    return new_pipeline;
  }

  /// Returns a new pipeline with a new value for fitted_.
  Pipeline with_fitted(const fct::Ref<const FittedPipeline>& _fitted) const {
    auto new_pipeline = *this;
    new_pipeline.fitted_ = _fitted.ptr();
    return new_pipeline;
  }

  /// Returns a new pipeline with new value for scores_.
  Pipeline with_scores(const fct::Ref<const metrics::Scores>& _scores) const {
    auto new_pipeline = *this;
    new_pipeline.scores_ = _scores;
    return new_pipeline;
  }

 public:
  /// Generates the placeholder and the peripheral names, integrating the
  /// many-to-one joins and all other modifications.
  std::pair<fct::Ref<const helpers::Placeholder>,
            fct::Ref<const std::vector<std::string>>>
  make_placeholder() const;

  /// Returns the names of the peripheral tables.
  fct::Ref<std::vector<std::string>> parse_peripheral() const;

  /// Parses the population name.
  std::shared_ptr<std::string> parse_population() const;

 private:
  /// Whether the pipeline is allowed to handle HTTP requests.
  bool allow_http_;

  /// Date and time of creation, expressed as a string
  std::string creation_time_;

  /// The parameters for the fitted pipeline.
  std::shared_ptr<const FittedPipeline> fitted_;

  /// Whether we want to include categorical features
  bool include_categorical_;

  /// The JSON object used to construct the pipeline.
  fct::Ref<const commands::Pipeline> obj_;

  /// The scores used to evaluate this pipeline
  fct::Ref<const metrics::Scores> scores_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_PIPELINE_HPP_
