#ifndef FASTPROP_ALGORITHM_ALGORITHM_HPP_
#define FASTPROP_ALGORITHM_ALGORITHM_HPP_

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

#include "fastprop/Float.hpp"
#include "fastprop/Int.hpp"

#include "fastprop/containers/containers.hpp"

#include "fastprop/Hyperparameters.hpp"

// ----------------------------------------------------------------------------
// Module files

#include "fastprop/algorithm/TableHolder.hpp"

#include "fastprop/algorithm/Aggregator.hpp"
#include "fastprop/algorithm/ConditionParser.hpp"
#include "fastprop/algorithm/RSquared.hpp"

#include "fastprop/algorithm/FastProp.hpp"

// ----------------------------------------------------------------------------

#endif  // FASTPROP_ALGORITHM_ALGORITHM_HPP_
