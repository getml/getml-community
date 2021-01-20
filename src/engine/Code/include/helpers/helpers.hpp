#ifndef HELPERS_HELPERS_HPP_
#define HELPERS_HELPERS_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <cmath>

#include <algorithm>
#include <array>
#include <memory>
#include <numeric>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Poco/JSON/Object.h>

#include "debug/debug.hpp"

#include "jsonutils/jsonutils.hpp"

#include "strings/strings.hpp"

// ----------------------------------------------------------------------------

// These are just typedefs
#include "helpers/Float.hpp"
#include "helpers/Int.hpp"

#include "helpers/NullChecker.hpp"

#include "helpers/ColumnOperators.hpp"

#include "helpers/StringReplacer.hpp"
#include "helpers/StringSplitter.hpp"

#include "helpers/Index.hpp"

#include "helpers/Column.hpp"
#include "helpers/IntSet.hpp"
#include "helpers/Placeholder.hpp"

#include "helpers/ColumnDescription.hpp"

#include "helpers/ImportanceMaker.hpp"

#include "helpers/ColumnView.hpp"

#include "helpers/DataFrame.hpp"

#include "helpers/DataFrameView.hpp"

#include "helpers/Matchmaker.hpp"

#include "helpers/TableHolder.hpp"

#include "helpers/Macros.hpp"

#include "helpers/SQLGenerator.hpp"

// ----------------------------------------------------------------------------

#endif  // HELPERS_HELPERS_HPP_
