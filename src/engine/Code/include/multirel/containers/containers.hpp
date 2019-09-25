#ifndef MULTIREL_CONTAINERS_HPP_
#define MULTIREL_CONTAINERS_HPP_

// ----------------------------------------------------
// Dependencies

#include <cmath>

#include <algorithm>
#include <array>
#include <map>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <Poco/JSON/Object.h>

#include "debug/debug.hpp"

#include "multirel/Float.hpp"
#include "multirel/Int.hpp"

#include "multirel/JSON.hpp"

// ----------------------------------------------------
// Module files

// These are just typedefs.
#include "multirel/containers/Features.hpp"
#include "multirel/containers/Index.hpp"
#include "multirel/containers/Match.hpp"
#include "multirel/containers/MatchPtrs.hpp"
#include "multirel/containers/Matches.hpp"

#include "multirel/containers/CategoryIndex.hpp"
#include "multirel/containers/IntSet.hpp"
#include "multirel/containers/Optional.hpp"

#include "multirel/containers/Column.hpp"
#include "multirel/containers/Schema.hpp"

#include "multirel/containers/ColumnView.hpp"

#include "multirel/containers/Predictions.hpp"

#include "multirel/containers/Subfeatures.hpp"

#include "multirel/containers/DataFrame.hpp"

#include "multirel/containers/DataFrameView.hpp"

// ----------------------------------------------------

#endif  // MULTIREL_CONTAINERS_HPP_
