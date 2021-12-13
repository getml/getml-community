#ifndef MEMMAP_MEMMAP_HPP_
#define MEMMAP_MEMMAP_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <optional>
#include <utility>

#include <Poco/File.h>
#include <Poco/TemporaryFile.h>

#include "debug/debug.hpp"

#include "strings/strings.hpp"

// ----------------------------------------------------------------------------

#include "memmap/Page.hpp"

#include "memmap/Pool.hpp"

#include "memmap/VectorImpl.hpp"

#include "memmap/Vector.hpp"

#include "memmap/BTreeNode.hpp"
#include "memmap/StringVector.hpp"

#include "memmap/BTree.hpp"

#include "memmap/Index.hpp"

// ----------------------------------------------------------------------------

#endif  // MEMMAP_MEMMAP_HPP_
