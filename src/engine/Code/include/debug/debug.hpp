#ifndef DEBUG_DEBUG_HPP_
#define DEBUG_DEBUG_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#ifdef __GNUG__
#include <cxxabi.h>
#include <execinfo.h>
#endif

#include <cassert>

#include <chrono>
#include <ctime>
#include <iostream>
#include <string>

// ----------------------------------------------------------------------------

#include "debug/StackTrace.hpp"

#include "debug/Assert.hpp"
#include "debug/Debugger.hpp"

#include "debug/assert_msg.hpp"
#include "debug/assert_true.hpp"
#include "debug/debug_log.hpp"
#include "debug/throw_unless.hpp"

// ----------------------------------------------------------------------------

#endif  // DEBUG_DEBUG_HPP_
