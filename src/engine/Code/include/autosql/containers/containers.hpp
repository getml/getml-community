#ifndef AUTOSQL_CONTAINERS_HPP_
#define AUTOSQL_CONTAINERS_HPP_

// ----------------------------------------------------
// Dependencies

#include <cassert>
#include <cmath>

#include <algorithm>
#include <array>
#include <map>
#include <memory>
#include <type_traits>
#include <vector>
#include <unordered_map>

#include <Poco/JSON/Object.h>

#include "debug/debug.hpp"

#include "autosql/Float.hpp"
#include "autosql/Int.hpp"

#include "autosql/JSON.hpp"

// ----------------------------------------------------
// Module files

// These are just typedefs.
#include "autosql/containers/Index.hpp"
#include "autosql/containers/Match.hpp"
#include "autosql/containers/MatchPtrs.hpp"
#include "autosql/containers/Matches.hpp"

#include "autosql/containers/CategoryIndex.hpp"
#include "autosql/containers/IntSet.hpp"
#include "autosql/containers/Optional.hpp"

#include "autosql/containers/Column.hpp"
#include "autosql/containers/Schema.hpp"

#include "autosql/containers/ColumnView.hpp"

#include "autosql/containers/Predictions.hpp"

#include "autosql/containers/Subfeatures.hpp"

#include "autosql/containers/DataFrame.hpp"

#include "autosql/containers/DataFrameView.hpp"

// ----------------------------------------------------

#endif  // AUTOSQL_CONTAINERS_HPP_
