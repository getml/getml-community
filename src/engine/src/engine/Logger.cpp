#include "engine/communication/Logger.hpp"

// ------------------------------------------------------------------------

#include "engine/communication/Sender.hpp"

// ------------------------------------------------------------------------

namespace engine {
namespace communication {

void Logger::log(const std::string& _msg) const {
  assert_true(monitor_);

  const auto now = std::chrono::system_clock::now();

  const std::time_t current_time = std::chrono::system_clock::to_time_t(now);

  std::cout << std::ctime(&current_time) << _msg << std::endl << std::endl;
}

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine
