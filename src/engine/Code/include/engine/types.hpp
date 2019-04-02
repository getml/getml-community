#ifndef ENGINE_TYPES_HPP_
#define ENGINE_TYPES_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <cstdint>

// ----------------------------------------------------------------------------

#define ENGINE_INT std::int32_t
#define ENGINE_FLOAT double
#define ENGINE_UNSIGNED_LONG std::uint_least64_t

#define ENGINE_INDEX std::unordered_map<size_t, std::vector<size_t>>

// ----------------------------------------------------------------------------

#endif  // ENGINE_TYPES_HPP_