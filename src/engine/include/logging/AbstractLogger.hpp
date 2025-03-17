// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef LOGGING_ABSTRACTLOGGER_HPP_
#define LOGGING_ABSTRACTLOGGER_HPP_

#include <string>

namespace logging {
// ------------------------------------------------------------------------

class AbstractLogger {
  // --------------------------------------------------------

 public:
  AbstractLogger() {}

  virtual ~AbstractLogger() = default;

  // --------------------------------------------------------

  /// Logs current events.
  virtual void log(const std::string& _msg) const = 0;

  // ----------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace logging

#endif  // LOGGING_ABSTRACTLOGGER_HPP_
