#ifndef HELPERS_HELPERS_HPP_
#define HELPERS_HELPERS_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <cmath>

#include <algorithm>
#include <array>
#include <functional>
#include <memory>
#include <numeric>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Poco/JSON/Object.h>

#include "debug/debug.hpp"

#include "io/io.hpp"

#include "jsonutils/jsonutils.hpp"

#include "logging/logging.hpp"

#include "stl/stl.hpp"

#include "strings/strings.hpp"

#include "textmining/textmining.hpp"

// ----------------------------------------------------------------------------

// These are just typedefs
#include "helpers/Float.hpp"
#include "helpers/Int.hpp"

#include "helpers/Subrole.hpp"

#include "helpers/SubroleParser.hpp"

#include "helpers/NullChecker.hpp"

#include "helpers/Aggregations.hpp"

#include "helpers/StringReplacer.hpp"
#include "helpers/StringSplitter.hpp"

#include "helpers/Index.hpp"

#include "helpers/Column.hpp"
#include "helpers/IntSet.hpp"
#include "helpers/Placeholder.hpp"
#include "helpers/Schema.hpp"

#include "helpers/VocabularyTree.hpp"

#include "helpers/FeatureContainer.hpp"

#include "helpers/ColumnDescription.hpp"

#include "helpers/ImportanceMaker.hpp"

#include "helpers/ColumnView.hpp"

#include "helpers/DataFrame.hpp"

#include "helpers/DataFrameView.hpp"

#include "helpers/Matchmaker.hpp"

#include "helpers/VocabularyContainer.hpp"

#include "helpers/WordIndexContainer.hpp"

#include "helpers/RowIndexContainer.hpp"

#include "helpers/TableHolder.hpp"

#include "helpers/Macros.hpp"

#include "helpers/SQLGenerator.hpp"

#include "helpers/FitParams.hpp"
#include "helpers/TransformParams.hpp"

// ----------------------------------------------------------------------------

#endif  // HELPERS_HELPERS_HPP_
