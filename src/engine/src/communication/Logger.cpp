// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "communication/Logger.hpp"

#include "communication/Sender.hpp"

namespace communication {

void Logger::log(const std::string& _msg) const {
  assert_true(monitor_);

  const auto now = std::chrono::system_clock::now();

  const std::time_t current_time = std::chrono::system_clock::to_time_t(now);

  std::cout << std::ctime(&current_time) << _msg << std::endl << std::endl;
}

}  // namespace communication
