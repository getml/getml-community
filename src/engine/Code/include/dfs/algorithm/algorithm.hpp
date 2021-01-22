#ifndef DFS_ALGORITHM_ALGORITHM_HPP_
#define DFS_ALGORITHM_ALGORITHM_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <execution>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <tuple>
#include <vector>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include "multithreading/multithreading.hpp"

#include "logging/logging.hpp"

#include "metrics/metrics.hpp"

#include "strings/strings.hpp"

#include "debug/debug.hpp"

#include "jsonutils/jsonutils.hpp"

#include "dfs/Float.hpp"
#include "dfs/Int.hpp"

#include "dfs/containers/containers.hpp"

#include "dfs/Hyperparameters.hpp"

// ----------------------------------------------------------------------------
// Module files

#include "dfs/algorithm/TableHolder.hpp"

#include "dfs/algorithm/Aggregator.hpp"
#include "dfs/algorithm/ConditionParser.hpp"
#include "dfs/algorithm/RSquared.hpp"

#include "dfs/algorithm/DeepFeatureSynthesis.hpp"

// ----------------------------------------------------------------------------

#endif  // DFS_ALGORITHM_ALGORITHM_HPP_
