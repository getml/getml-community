#ifndef TRANSPILATION_SQLPARAMS_HPP_
#define TRANSPILATION_SQLPARAMS_HPP_

#include <string>
#include <vector>

#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/define_named_tuple.hpp"
#include "rfl/remove_fields.hpp"
#include "transpilation/FeatureTableParams.hpp"

namespace transpilation {

/// The features expressed as SQL code.
using f_sql = rfl::Field<"sql_", std::vector<std::string>>;

/// Contains the parameters needed to create a feature table.
using SQLParams = rfl::define_named_tuple_t<
    rfl::remove_fields_t<FeatureTableParams, "prefix_">, f_sql>;

}  // namespace transpilation

#endif
