#ifndef HELPERS_INMEMORYINDEX_HPP_
#define HELPERS_INMEMORYINDEX_HPP_

// -------------------------------------------------------------------------

#include <cstddef>
#include <unordered_map>
#include <vector>

// -------------------------------------------------------------------------

#include "helpers/Int.hpp"

// -------------------------------------------------------------------------

namespace helpers {

typedef std::unordered_map<Int, std::vector<size_t>> InMemoryIndex;

}  // namespace helpers

#endif  // HELPERS_INMEMORYINDEX_HPP_

