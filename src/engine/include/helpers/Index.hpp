#ifndef HELPERS_INDEX_HPP_
#define HELPERS_INDEX_HPP_

// -------------------------------------------------------------------------

#include <variant>

// -------------------------------------------------------------------------

#include "helpers/InMemoryIndex.hpp"
#include "helpers/MemoryMappedIndex.hpp"

// -------------------------------------------------------------------------

namespace helpers {
// -------------------------------------------------------------------------

typedef std::variant<InMemoryIndex, MemoryMappedIndex> Index;

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_INDEX_HPP_
