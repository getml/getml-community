#ifndef STL_RANGES_HPP_
#define STL_RANGES_HPP_

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

#endif  // STL_RANGES_HPP_
