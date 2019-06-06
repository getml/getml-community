#ifndef AUTOSQL_DECISIONTREES_DECISIONTREES_HPP_
#define AUTOSQL_DECISIONTREES_DECISIONTREES_HPP_

// ----------------------------------------------------
// Dependencies

#include <cassert>
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

#include "autosql/types.hpp"

#include "autosql/descriptors/descriptors.hpp"

#include "autosql/JSON.hpp"

#include "autosql/Sample.hpp"

#include "autosql/containers/containers.hpp"

#include "autosql/aggregations/aggregations.hpp"

#include "autosql/lossfunctions/lossfunctions.hpp"

// ----------------------------------------------------
// Module files

#include "autosql/decisiontrees/RandomNumberGenerator.hpp"

#include "autosql/decisiontrees/TableHolder.hpp"

#include "autosql/decisiontrees/DecisionTreeImpl.hpp"

#include "autosql/decisiontrees/DecisionTreeNode.hpp"

// ----------------------------------------------------


/*#include "decisiontrees/DecisionTree.hpp"

#include "decisiontrees/TablePreparer.hpp"

#include "decisiontrees/SameUnitIdentifier.hpp"

#include "decisiontrees/CandidateTreeBuilder.hpp"

#include "decisiontrees/TreeFitter.hpp"

#include "decisiontrees/DecisionTreeEnsembleImpl.hpp"

#include "decisiontrees/DecisionTreeEnsemble.hpp"*/

#endif  // AUTOSQL_DECISIONTREES_DECISIONTREES_HPP_
