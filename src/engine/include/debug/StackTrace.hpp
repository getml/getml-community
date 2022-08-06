// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef DEBUG_STACKTRACE_HPP_
#define DEBUG_STACKTRACE_HPP_

// ----------------------------------------------------------------------------

#ifdef __GNUG__
#include <cxxabi.h>
#include <execinfo.h>
#endif

// ----------------------------------------------------------------------------

#include <string>

// ----------------------------------------------------------------------------

namespace debug {
// ------------------------------------------------------------------------

class StackTrace {
 public:
  /// Generates a stack trace.
  static std::string make() {
#ifdef __GNUG__
    constexpr size_t SIZE = 100;

    void* buffer[SIZE];

    const int nptrs = backtrace(buffer, SIZE);

    char** strings = backtrace_symbols(buffer, nptrs);

    if (strings == NULL) {
      return "";
    }

    std::string stack_trace;

    for (int j = 0; j < nptrs; ++j) {
      stack_trace += "#" + std::to_string(j + 1) + "  ";
      stack_trace += demangle(strings[j]);
      stack_trace += "\n\n";
    }

    free(strings);

    return stack_trace;
#else
    return "";
#endif
  }

 private:
  /// Returns C++ functions in more readable format.
  static std::string demangle(const std::string& _original) {
    const auto stripped = strip(_original);
#ifdef __GNUG__
    int status = 0;

    char* demangled_c_str =
        abi::__cxa_demangle(stripped.c_str(), 0, 0, &status);

    if (!demangled_c_str) {
      return stripped;
    }

    const std::string demangled = demangled_c_str;

    free(demangled_c_str);

    return demangled;
#else
    return stripped;
#endif
  }

  /// Removes the beginning and end of the stack trace function.
  static std::string strip(const std::string& _original) {
    size_t begin = 0;

    size_t end = _original.size();

    for (size_t i = 0; i < _original.size(); ++i) {
      if (_original[i] == '(') {
        begin = i + 1;
      } else if (_original[i] == '+') {
        end = i;
        break;
      } else if (_original[i] == ')') {
        end = i;
        break;
      }
    }

    if (end <= begin) {
      return _original;
    }

    return _original.substr(begin, end - begin);
  }
};

// ------------------------------------------------------------------------
}  // namespace debug

#endif  // DEBUG_STACKTRACE_HPP_
