
// ---------------------------------------------------------------------------

#include <fstream>
#include <iostream>
#include <random>

#include <Poco/JSON/Parser.h>

#include "debug/debug.hpp"

#include "multirel/JSON.hpp"
#include "multirel/containers/containers.hpp"
#include "multirel/descriptors/descriptors.hpp"
#include "multirel/ensemble/ensemble.hpp"

// ---------------------------------------------------------------------------

#include "multirel/tests/make_categorical_column.hpp"
#include "multirel/tests/make_column.hpp"

#include "multirel/tests/load_json.hpp"

#include "multirel/tests/test1_count.hpp"
#include "multirel/tests/test2_avg.hpp"
#include "multirel/tests/test3_sum.hpp"
#include "multirel/tests/test4_max.hpp"
#include "multirel/tests/test5_min.hpp"
#include "multirel/tests/test6_time_stamps_diff.hpp"
#include "multirel/tests/test7_categorical.hpp"
#include "multirel/tests/test8_multiple_categorical.hpp"
#include "multirel/tests/test9_discrete.hpp"

#include "multirel/tests/test10_same_units_categorical.hpp"
#include "multirel/tests/test11_same_units_numerical.hpp"
#include "multirel/tests/test12_same_units_discrete.hpp"

#include "multirel/tests/test13_categorical_output.hpp"
#include "multirel/tests/test14_numerical_output.hpp"
#include "multirel/tests/test15_discrete_output.hpp"

#include "multirel/tests/test16_nan_values_numerical.hpp"
#include "multirel/tests/test17_nan_values_discrete.hpp"

#include "multirel/tests/test18_upper_time_stamps.hpp"

#include "multirel/tests/test20_saving_and_loading_models.hpp"

#include "multirel/tests/test21_snowflake_model.hpp"

#include "multirel/tests/test22_time_windows.hpp"

// ---------------------------------------------------------------------------
