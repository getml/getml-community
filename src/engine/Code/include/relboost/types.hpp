#ifndef RELBOOST_TYPES_HPP_
#define RELBOOST_TYPES_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <cstdint>

#include <unordered_map>
#include <vector>

// ----------------------------------------------------------------------------

#define RELBOOST_INT std::int32_t
#define RELBOOST_FLOAT double
#define RELBOOST_UNSIGNED_INT std::uint_least64_t

#define RELBOOST_INDEX std::unordered_map<size_t, std::vector<size_t>>

#define RELBOOST_MATCH_CONTAINER std::vector<size_t*>

// ----------------------------------------------------------------------------

#endif  // RELBOOST_TYPES_HPP_