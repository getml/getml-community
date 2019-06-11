#ifndef AUTOSQL_CONTAINERS_HPP_
#define AUTOSQL_CONTAINERS_HPP_

// ----------------------------------------------------
// Dependencies

#include <cassert>
#include <cmath>

#include <algorithm>
#include <array>
#include <memory>
#include <type_traits>
#include <vector>

#include <Poco/JSON/Object.h>

#include "debug/debug.hpp"

#include "autosql/types.hpp"

#include "autosql/JSON.hpp"

#include "autosql/Sample.hpp"

// ----------------------------------------------------
// Module files

#include "autosql/containers/CategoryIndex.hpp"
#include "autosql/containers/IntSet.hpp"
#include "autosql/containers/Optional.hpp"

#include "autosql/containers/Column.hpp"
#include "autosql/containers/Schema.hpp"

#include "autosql/containers/ColumnView.hpp"

#include "autosql/containers/DataFrame.hpp"

#include "autosql/containers/DataFrameView.hpp"

// ----------------------------------------------------

#endif  // AUTOSQL_CONTAINERS_HPP_
