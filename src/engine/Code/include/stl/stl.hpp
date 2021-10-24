#ifndef STL_STL_HPP_
#define STL_STL_HPP_

#include <map>
#include <numeric>
#include <set>
#include <sstream>
#include <tuple>
#include <vector>

// TODO: Remove this temporary fix as soon as
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

#include <Poco/JSON/Array.h>

#include "multithreading/multithreading.hpp"

#include "stl/IotaIterator.hpp"

#include "stl/IotaRange.hpp"

#include "stl/iota.hpp"

#include "stl/Range.hpp"
#include "stl/collect.hpp"

#ifndef __APPLE__
#include "stl/collect_parallel.hpp"
#endif

#include "stl/join.hpp"

#endif  // STL_STL_HPP_
