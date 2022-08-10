// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef FCT_RANGES_HPP_
#define FCT_RANGES_HPP_

// TODO(patrick): Remove this temporary fix as soon as
// clang fully supports the ranges library.
#ifdef __APPLE__

#include <range/v3/all.hpp>
#define RANGES ranges
#define VIEWS ranges::views

#else

#include <ranges>
#define RANGES std::ranges
#define VIEWS std::views

#endif

#endif  // FCT_RANGES_HPP_
