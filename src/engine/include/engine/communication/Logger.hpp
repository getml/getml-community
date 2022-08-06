// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_COMMUNICATION_LOGGER_HPP_
#define ENGINE_COMMUNICATION_LOGGER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <string>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "logging/logging.hpp"

// ----------------------------------------------------------------------------

#include "engine/communication/Monitor.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace communication {

// ------------------------------------------------------------------------

class Logger : public logging::AbstractLogger {
 public:
  Logger(const std::shared_ptr<const Monitor>& _monitor) : monitor_(_monitor) {
    assert_true(monitor_);
  }

  ~Logger() = default;

  // --------------------------------------------------------

  /// Logs current events.
  void log(const std::string& _msg) const final;

  // ----------------------------------------------------

 private:
  /// The Monitor is supposed to monitor all of the logs as well.
  const std::shared_ptr<const Monitor> monitor_;

  // ----------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

#endif  // ENGINE_COMMUNICATION_LOGGER_HPP_
