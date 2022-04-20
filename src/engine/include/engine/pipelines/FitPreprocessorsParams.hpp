#ifndef ENGINE_PIPELINES_FITPREPROCESSORSPARAMS_HPP_
#define ENGINE_PIPELINES_FITPREPROCESSORSPARAMS_HPP_

// ----------------------------------------------------------------------------

#include <Poco/Net/StreamSocket.h>

// ----------------------------------------------------------------------------

#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"
#include "engine/dependency/dependency.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

struct FitPreprocessorsParams {
  /// The categorical encoding.
  const std::shared_ptr<containers::Encoding> categories_;

  /// The command used.
  const Poco::JSON::Object cmd_;

  /// Logs the progress.
  const std::shared_ptr<const communication::Logger> logger_;

  /// The peripheral tables.
  const std::vector<containers::DataFrame> peripheral_dfs_;

  /// The population table.
  const containers::DataFrame population_df_;

  /// The dependency tracker for the preprocessors.
  const std::shared_ptr<dependency::PreprocessorTracker> preprocessor_tracker_;

  /// Output: The socket with which we communicate.
  Poco::Net::StreamSocket* const socket_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPREPROCESSORSPARAMS_HPP_

