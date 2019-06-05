#ifndef AUTOSQL_CONTAINERS_HPP_
#define AUTOSQL_CONTAINERS_HPP_

// ----------------------------------------------------
// Dependencies

#include <cassert>
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

#include "Poco/JSON/Object.h"

#include <Poco/File.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Path.h>

#include "types.hpp"

#include "config.hpp"

#ifdef AUTOSQL_MULTITHREADING
#include "multithreading/multithreading.hpp"
#endif  // AUTOSQL_MULTITHREADING

#include "debug.hpp"

#include "Endianness.hpp"

#include "JSON.hpp"

#include "Sample.hpp"

// ----------------------------------------------------
// Module files

#include "containers/CategoryIndex.hpp"

#include "containers/Encoding.hpp"

#include "containers/Matrix.hpp"

#include "containers/ColumnView.hpp"

#include "containers/MatrixView.hpp"

#include "containers/DataFrame.hpp"

#include "containers/DataFrameView.hpp"

#include "containers/Summarizer.hpp"

#include "containers/IntSet.hpp"

#include "containers/Optional.hpp"

// ----------------------------------------------------

#endif  // AUTOSQL_CONTAINERS_HPP_
