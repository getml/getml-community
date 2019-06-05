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

#ifdef AUTOSQL_MULTITHREADING
#include <atomic>
#include <future>
#include <thread>
#endif  // AUTOSQL_MULTITHREADING

#ifdef AUTOSQL_MULTINODE_MPI
#include <boost/mpi.hpp>
#include <boost/mpi/collectives.hpp>
#endif  // AUTOSQL_MULTINODE_MPI

#include "Poco/JSON/Object.h"

#include <Poco/File.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Path.h>

#include <xgboost/c_api.h>

#include "config.hpp"

#include "types.hpp"

#include "debug.hpp"

#include "predictors/predictors.hpp"

#include "descriptors/descriptors.hpp"

#ifdef AUTOSQL_MULTITHREADING
#include "multithreading/multithreading.hpp"
#endif  // AUTOSQL_MULTITHREADING

#ifdef AUTOSQL_MULTINODE_MPI
#include "MPI/Sendcounts.hpp"
#endif  // AUTOSQL_MULTINODE_MPI

#include "Endianness.hpp"

#include "Placeholder.hpp"

#include "JSON.hpp"

#include "containers/containers.hpp"

#include "Sample.hpp"

#include "optimizationcriteria/optimizationcriteria.hpp"

#include "aggregations/aggregations.hpp"

#include "SampleContainer.hpp"

#include "lossfunctions/lossfunctions.hpp"

#include "LinearRegression.hpp"

#include "logging/logging.hpp"

#include "metrics/metrics.hpp"

// ----------------------------------------------------
// Module files

#include "decisiontrees/RandomNumberGenerator.hpp"

#include "decisiontrees/TableHolder.hpp"

#include "decisiontrees/DecisionTreeImpl.hpp"

#include "decisiontrees/DecisionTreeNode.hpp"

#include "decisiontrees/DecisionTree.hpp"

#include "decisiontrees/TablePreparer.hpp"

#include "decisiontrees/SameUnitIdentifier.hpp"

#include "decisiontrees/CandidateTreeBuilder.hpp"

#include "decisiontrees/TreeFitter.hpp"

#include "decisiontrees/DecisionTreeEnsembleImpl.hpp"

#include "decisiontrees/DecisionTreeEnsemble.hpp"

#endif  // AUTOSQL_DECISIONTREES_DECISIONTREES_HPP_
