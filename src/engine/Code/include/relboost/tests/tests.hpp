
// ---------------------------------------------------------------------------

#include <fstream>
#include <iostream>
#include <random>

#include <Poco/JSON/Parser.h>

#include "relboost/Hyperparameters.hpp"
#include "relboost/JSON.hpp"
#include "relboost/containers/containers.hpp"
#include "relboost/ensemble/ensemble.hpp"

// ---------------------------------------------------------------------------

#include "relboost/tests/make_categorical_column.hpp"
#include "relboost/tests/make_column.hpp"

#include "relboost/tests/load_json.hpp"

#include "relboost/tests/test1_sum.hpp"
#include "relboost/tests/test2_avg.hpp"
#include "relboost/tests/test3_avg.hpp"
#include "relboost/tests/test4_time_stamps_diff.hpp"
#include "relboost/tests/test5_categorical.hpp"
#include "relboost/tests/test6_categorical.hpp"
#include "relboost/tests/test7_categorical.hpp"
#include "relboost/tests/test8_multiple_categorical.hpp"
#include "relboost/tests/test9_discrete.hpp"

#include "relboost/tests/test10_same_units_categorical.hpp"
#include "relboost/tests/test11_same_units_numerical.hpp"
#include "relboost/tests/test12_same_units_discrete.hpp"
#include "relboost/tests/test13_numerical_output.hpp"
#include "relboost/tests/test14_categorical_output.hpp"
#include "relboost/tests/test15_discrete_output.hpp"
#include "relboost/tests/test16_nan_values_numerical.hpp"
#include "relboost/tests/test17_nan_values_discrete.hpp"
#include "relboost/tests/test18_upper_time_stamps.hpp"
#include "relboost/tests/test19_classification.hpp"

#include "relboost/tests/test20_saving_and_loading.hpp"
#include "relboost/tests/test21_time_windows.hpp"
#include "relboost/tests/test22_snowflake_model.hpp"
#include "relboost/tests/test23_snowflake_model2.hpp"

// ---------------------------------------------------------------------------
