#ifndef ENGINE_CONTAINERS_HPP_
#define ENGINE_CONTAINERS_HPP_

// ----------------------------------------------------
// Dependencies

#include <cassert>
#include <cstdint>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Poco/JSON/Object.h>

#include <Poco/File.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Path.h>

#include "debug/debug.hpp"

#include "database/database.hpp"

#include "multithreading/multithreading.hpp"

#include "engine/types.hpp"

#include "engine/JSON.hpp"

#include "engine/utils/utils.hpp"

// ----------------------------------------------------
// Module files

#include "engine/containers/Encoding.hpp"
#include "engine/containers/Index.hpp"
#include "engine/containers/Matrix.hpp"

#include "engine/containers/DataFrameIndex.hpp"

#include "engine/containers/DataFrame.hpp"

// ----------------------------------------------------

#endif  // ENGINE_CONTAINERS_HPP_
