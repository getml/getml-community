#ifndef ENGINE_PIPELINES_SAVEPARAMS_HPP_
#define ENGINE_PIPELINES_SAVEPARAMS_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <vector>

// ----------------------------------------------------------------------------

#include "engine/containers/containers.hpp"
#include "engine/dependency/dependency.hpp"

// ----------------------------------------------------------------------------

#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/Pipeline.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

struct SaveParams {
  /// Encodes the categories.
  const helpers::StringIterator categories_;

  /// The fitted pipeline.
  const FittedPipeline fitted_;

  /// The name of the pipeline to be save.
  const std::string name_;

  /// The path in which to save the final result.
  const std::string path_;

  /// The underlying pipeline,
  const Pipeline pipeline_;

  /// A path to a temporary directory.
  const std::string temp_dir_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_SAVEPARAMS_HPP_
