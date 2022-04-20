#ifndef ENGINE_PIPELINES_PIPELINEJSON_HPP_
#define ENGINE_PIPELINES_PIPELINEJSON_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <vector>

// ----------------------------------------------------------------------------

#include "engine/pipelines/Fingerprints.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

struct PipelineJSON {
  /// Whether the pipeline is allowed to handle HTTP requests.
  const bool allow_http_;

  /// Date and time of creation, expressed as a string
  const std::string creation_time_;

  /// The fingerprints required for the fitted pipeline.
  const Fingerprints fingerprints_;

  /// The schema of the peripheral tables as they are inserted into the
  /// feature learners.
  const fct::Ref<const std::vector<helpers::Schema>>
      modified_peripheral_schema_;

  /// The schema of the population as it is inserted into the feature
  /// learners.
  const fct::Ref<const helpers::Schema> modified_population_schema_;

  /// The schema of the peripheral tables as they are originally passed.
  const fct::Ref<const std::vector<helpers::Schema>> peripheral_schema_;

  /// The schema of the population as originally passed.
  const fct::Ref<const helpers::Schema> population_schema_;

  /// The names of the targets.
  const std::vector<std::string> targets_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_PIPELINEJSON_HPP_

