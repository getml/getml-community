#ifndef RELBOOST_UTILS_LOGGER_HPP_
#define RELBOOST_UTILS_LOGGER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <string>

// ----------------------------------------------------------------------------

#include "logging/logging.hpp"
#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------
namespace relboost {
namespace utils {
// ----------------------------------------------------------------------------

struct Logger {
  // ------------------------------------------------------------------------

  /// Logs the message both on the monitor and in the API.
  static void log(const std::string& _msg,
                  const std::shared_ptr<const logging::AbstractLogger> _logger,
                  multithreading::Communicator* _comm) {
    if (_logger) {
      try {
        _logger->log(_msg);
      } catch (std::exception& e) {
        _comm->checkpoint(false);
      }
    }

    _comm->checkpoint(true);
  }

  // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_REDUCER_HPP_
