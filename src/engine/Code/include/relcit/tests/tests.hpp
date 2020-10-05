
// ---------------------------------------------------------------------------

#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>

#include <Poco/JSON/Parser.h>
#include <Poco/TemporaryFile.h>

#include "relcit/Hyperparameters.hpp"
#include "relcit/JSON.hpp"
#include "relcit/containers/containers.hpp"
#include "relcit/ensemble/ensemble.hpp"

// ---------------------------------------------------------------------------

#include "relcit/tests/make_categorical_column.hpp"
#include "relcit/tests/make_column.hpp"

#include "relcit/tests/load_json.hpp"

#include "relcit/tests/test1_sum.hpp"
#include "relcit/tests/test2_count.hpp"
#include "relcit/tests/test3_avg.hpp"
#include "relcit/tests/test4_categorical.hpp"
#include "relcit/tests/test5_multiple_categorical.hpp"
#include "relcit/tests/test6_snowflake_model.hpp"

// ---------------------------------------------------------------------------
