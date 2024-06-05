#ifndef TRANSPILATION_FEATURETABLEPARAMS_HPP_
#define TRANSPILATION_FEATURETABLEPARAMS_HPP_

#include <string>
#include <vector>

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>

namespace transpilation {

/// The main table from which we want to create the features.
using f_main_table = rfl::Field<"main_table_", std::string>;

/// The names of the automatically generated features.
using f_autofeatures = rfl::Field<"autofeatures_", std::vector<std::string>>;

/// The names of the targets.
using f_targets = rfl::Field<"targets_", std::vector<std::string>>;

/// The names of the categorical features.
using f_categorical = rfl::Field<"categorical_", std::vector<std::string>>;

/// The names of the numerical features.
using f_numerical = rfl::Field<"numerical_", std::vector<std::string>>;

/// The prefix used for the faeture table.
using f_prefix = rfl::Field<"prefix_", std::string>;

/// Contains the parameters needed to create a feature table.
using FeatureTableParams =
    rfl::NamedTuple<f_main_table, f_autofeatures, f_targets, f_categorical,
                    f_numerical, f_prefix>;

}  // namespace transpilation

#endif
