
// ---------------------------------------------------------------------------

#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>

#include <Poco/JSON/Parser.h>
#include <Poco/TemporaryFile.h>

#include "relmt/Hyperparameters.hpp"
#include "relmt/JSON.hpp"
#include "relmt/containers/containers.hpp"
#include "relmt/ensemble/ensemble.hpp"

// ---------------------------------------------------------------------------

#include "relmt/tests/make_categorical_column.hpp"
#include "relmt/tests/make_column.hpp"

#include "relmt/tests/load_json.hpp"

#include "relmt/tests/test1_sum.hpp"
#include "relmt/tests/test2_count.hpp"
#include "relmt/tests/test3_avg.hpp"
#include "relmt/tests/test4_categorical.hpp"
#include "relmt/tests/test5_multiple_categorical.hpp"
#include "relmt/tests/test6_snowflake_model.hpp"

// ---------------------------------------------------------------------------
