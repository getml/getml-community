#ifndef DEBUG_DEBUGGER_HPP_
#define DEBUG_DEBUGGER_HPP_

// ----------------------------------------------------------------------------

#include <chrono>
#include <iostream>
#include <string>

// ----------------------------------------------------------------------------

namespace debug {
// ------------------------------------------------------------------------

struct Debugger {
  /// Prints a debug message on the screen.
  static void log(const std::string& _msg) {
    auto now = std::chrono::system_clock::now();

    std::time_t current_time = std::chrono::system_clock::to_time_t(now);

    std::cout << std::ctime(&current_time) << "DEBUG: " << _msg << std::endl
              << std::endl;
  }
};

// ------------------------------------------------------------------------
}  // namespace debug

#endif  // DEBUG_DEBUGGER_HPP_
