
// ---------------------------------------------------------------------------

#include <fstream>
#include <iostream>
#include <random>

#include <Poco/JSON/Parser.h>

#include "autosql/JSON.hpp"
#include "autosql/containers/containers.hpp"
#include "autosql/descriptors/descriptors.hpp"
#include "autosql/ensemble/ensemble.hpp"

// ---------------------------------------------------------------------------

#include "autosql/tests/make_categorical_column.hpp"
#include "autosql/tests/make_column.hpp"

#include "autosql/tests/load_json.hpp"

#include "autosql/tests/test1_count.hpp"
#include "autosql/tests/test2_avg.hpp"
#include "autosql/tests/test3_sum.hpp"
#include "autosql/tests/test4_max.hpp"
#include "autosql/tests/test5_min.hpp"
#include "autosql/tests/test6_time_stamps_diff.hpp"
#include "autosql/tests/test7_categorical.hpp"
#include "autosql/tests/test8_multiple_categorical.hpp"
#include "autosql/tests/test9_discrete.hpp"

#include "autosql/tests/test10_same_units_categorical.hpp"
#include "autosql/tests/test11_same_units_numerical.hpp"
#include "autosql/tests/test12_same_units_discrete.hpp"

#include "autosql/tests/test13_categorical_output.hpp"
#include "autosql/tests/test14_numerical_output.hpp"
#include "autosql/tests/test15_discrete_output.hpp"

// ---------------------------------------------------------------------------
