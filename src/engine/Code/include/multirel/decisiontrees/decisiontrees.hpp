#ifndef MULTIREL_DECISIONTREES_DECISIONTREES_HPP_
#define MULTIREL_DECISIONTREES_DECISIONTREES_HPP_

// ----------------------------------------------------
// Dependencies

#include <cmath>
#include <cstdint>
#include <numeric>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <atomic>
#include <future>
#include <thread>

#include "Poco/JSON/Object.h"

#include <Poco/File.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Path.h>

#include "debug/debug.hpp"

#include "multithreading/multithreading.hpp"

#include "multirel/Float.hpp"
#include "multirel/Int.hpp"

#include "multirel/descriptors/descriptors.hpp"

#include "multirel/JSON.hpp"

#include "multirel/containers/containers.hpp"

#include "multirel/aggregations/aggregations.hpp"

#include "multirel/lossfunctions/lossfunctions.hpp"

#include "multirel/utils/utils.hpp"

// ----------------------------------------------------
// Module files

#include "multirel/decisiontrees/Placeholder.hpp"

#include "multirel/decisiontrees/TableHolder.hpp"

#include "multirel/decisiontrees/DecisionTreeImpl.hpp"

#include "multirel/decisiontrees/DecisionTreeNode.hpp"

#include "multirel/decisiontrees/DecisionTree.hpp"

// ----------------------------------------------------

#endif  // MULTIREL_DECISIONTREES_DECISIONTREES_HPP_
