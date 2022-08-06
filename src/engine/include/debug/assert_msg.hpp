// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef DEBUG_ASSERT_MSG_HPP_
#define DEBUG_ASSERT_MSG_HPP_

// ----------------------------------------------------------------------------

// NDEBUG implies no assertions.
#ifdef NDEBUG

#define assert_msg(EX, CUSTOM_MSG)

#else

#define assert_msg(EX, CUSTOM_MSG)                                             \
  (void)((EX) ||                                                               \
         (debug::Assert::throw_exception(#EX, __FILE__, __LINE__, CUSTOM_MSG), \
          0))

#endif

// ----------------------------------------------------------------------------

#endif  // DEBUG_ASSERT_MSG_HPP_
