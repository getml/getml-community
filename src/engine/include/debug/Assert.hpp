// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef DEBUG_ASSERT_HPP_
#define DEBUG_ASSERT_HPP_

// ----------------------------------------------------------------------------

#include <stdexcept>
#include <string>

// ----------------------------------------------------------------------------

#include "debug/StackTrace.hpp"

// ----------------------------------------------------------------------------

namespace debug {
// ------------------------------------------------------------------------

struct Assert {
  /// Throws an exception providing the file and the line.
  static void throw_exception(const char *_msg, const char *_file,
                              const int _line) {
    Assert::throw_exception(_msg, _file, _line, "");
  }

  /// Throws an exception providing the file and the line as well as a
  /// custom message.
  static void throw_exception(const char *_msg, const char *_file,
                              const int _line, const std::string &_custom_msg) {
    const auto stack_trace = StackTrace::make();

    const auto custom_msg =
        _custom_msg == "" ? _custom_msg : _custom_msg + ".\n\n";

    const auto output = std::string("Assertion failed: ") + _msg + " at " +
                        _file + ", line " + std::to_string(_line) + ".\n\n" +
                        custom_msg + stack_trace +
                        "Please help us improve our software by reporting this "
                        "incident.";

    throw_exception(output);
  }

  /// Throws an exception based on the user-defined message.
  static void throw_exception(const std::string &_msg) {
    throw std::runtime_error(_msg);
  }
};

// ------------------------------------------------------------------------
}  // namespace debug

#endif  // DEBUG_ASSERT_HPP_
