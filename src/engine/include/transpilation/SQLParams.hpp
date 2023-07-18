#ifndef TRANSPILATION_SQLPARAMS_HPP_
#define TRANSPILATION_SQLPARAMS_HPP_

#include <string>
#include <vector>

#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/define_named_tuple.hpp"
#include "fct/remove_fields.hpp"
#include "transpilation/FeatureTableParams.hpp"

namespace transpilation {

/// The features expressed as SQL code.
using f_sql = fct::Field<"sql_", std::vector<std::string>>;

/// Contains the parameters needed to create a feature table.
using SQLParams = fct::define_named_tuple_t<
    fct::remove_fields_t<FeatureTableParams, "prefix_">, f_sql>;

}  // namespace transpilation

#endif
