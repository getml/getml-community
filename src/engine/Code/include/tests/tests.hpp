
// ---------------------------------------------------------------------------

#include <fstream>
#include <iostream>
#include <random>

#include <Poco/JSON/Parser.h>

#include "Hyperparameters.hpp"
#include "JSON.hpp"
#include "containers/containers.hpp"
#include "ensemble/ensemble.hpp"

// ---------------------------------------------------------------------------

#include "tests/make_categorical_column.hpp"
#include "tests/make_column.hpp"

#include "tests/load_json.hpp"

#include "tests/test1_sum.hpp"
#include "tests/test2_avg.hpp"
#include "tests/test3_avg.hpp"
#include "tests/test4_time_stamps_diff.hpp"
#include "tests/test5_categorical.hpp"
#include "tests/test6_categorical.hpp"
#include "tests/test7_categorical.hpp"
#include "tests/test8_multiple_categorical.hpp"
#include "tests/test9_discrete.hpp"

#include "tests/test10_same_units_categorical.hpp"
#include "tests/test11_same_units_numerical.hpp"
#include "tests/test12_same_units_discrete.hpp"
#include "tests/test13_numerical_output.hpp"
#include "tests/test14_categorical_output.hpp"
#include "tests/test15_discrete_output.hpp"
#include "tests/test16_nan_values_numerical.hpp"
#include "tests/test17_nan_values_discrete.hpp"
#include "tests/test18_upper_time_stamps.hpp"

// ---------------------------------------------------------------------------
