// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef DEBUG_THROW_UNLESS_HPP_
#define DEBUG_THROW_UNLESS_HPP_

// ----------------------------------------------------------------------------

// Throw an exception unless condition is true.
#define throw_unless(_condition, _msg) \
  (void)((_condition) || (debug::Assert::throw_exception(_msg), 0))

// ----------------------------------------------------------------------------

#endif  // DEBUG_ASSERT_TRUE_HPP
