// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DEBUG_ASSERT_TRUE_HPP_
#define DEBUG_ASSERT_TRUE_HPP_

// NDEBUG implies no assertions.
#ifdef NDEBUG

#define assert_true(EX)

#else

#include "debug/Assert.hpp"

#define assert_true(EX) \
  (void)((EX) || (debug::Assert::throw_exception(#EX, __FILE__, __LINE__), 0))

#endif

// ----------------------------------------------------------------------------

#endif  // DEBUG_ASSERT_TRUE_HPP_
