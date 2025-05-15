// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "logging/ProgressLogger.hpp"

namespace logging {

ProgressLogger::ProgressLogger(
    const std::string& _msg,
    const std::shared_ptr<const AbstractLogger>& _logger, const size_t _total,
    const size_t _begin, const size_t _end)
    : begin_(_begin),
      current_value_(0),
      end_(_end),
      logger_(_logger),
      total_(_total) {
  if (logger_ && total_ > 0 && _msg != "") {
    logger_->log(_msg);
  }
}

}  // namespace logging
