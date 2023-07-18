#ifndef TRANSPILATION_FEATURETABLEPARAMS_HPP_
#define TRANSPILATION_FEATURETABLEPARAMS_HPP_

#include <string>
#include <vector>

#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace transpilation {

/// The main table from which we want to create the features.
using f_main_table = fct::Field<"main_table_", std::string>;

/// The names of the automatically generated features.
using f_autofeatures = fct::Field<"autofeatures_", std::vector<std::string>>;

/// The names of the targets.
using f_targets = fct::Field<"targets_", std::vector<std::string>>;

/// The names of the categorical features.
using f_categorical = fct::Field<"categorical_", std::vector<std::string>>;

/// The names of the numerical features.
using f_numerical = fct::Field<"numerical_", std::vector<std::string>>;

/// The prefix used for the faeture table.
using f_prefix = fct::Field<"prefix_", std::string>;

/// Contains the parameters needed to create a feature table.
using FeatureTableParams =
    fct::NamedTuple<f_main_table, f_autofeatures, f_targets, f_categorical,
                    f_numerical, f_prefix>;

}  // namespace transpilation

#endif
